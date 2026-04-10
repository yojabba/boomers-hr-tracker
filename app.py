import os
import sqlite3
import time
import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

# ===================== CONFIG =====================
SPORT_ID = 1
GAME_TYPE = "R"  # Regular season
POLL_SECONDS = 30
REQUEST_TIMEOUT = 20
MIN_GAMES_FOR_PROMO = 5
USER_AGENT = "Mozilla/5.0 (BoomersBigFliesTracker/LiveOnly)"

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/live"

PROMO_TZ = ZoneInfo("America/Los_Angeles")

DB_URL = os.getenv("DATABASE_URL")
SQLITE_PATH = Path("longest_hr_tracker.sqlite3")


def promo_today() -> date:
    """Get today's date in LA timezone, with fallback."""
    try:
        return datetime.now(PROMO_TZ).date()
    except Exception:
        # Fallback: assume UTC and subtract 7 hours for PDT
        from datetime import timedelta
        utc_now = datetime.utcnow()
        la_now = utc_now - timedelta(hours=7)  # PDT is UTC-7 (summer time)
        return la_now.date()

def get_current_time_la() -> datetime:
    """Get current time in LA timezone for display."""
    try:
        return datetime.now(PROMO_TZ)
    except Exception:
        from datetime import timedelta
        utc_now = datetime.utcnow()
        return utc_now - timedelta(hours=7)


# ===================== DATA CLASSES =====================
@dataclass
class HomeRunEvent:
    game_date: str
    game_pk: int
    batter: str
    team: str
    distance: int
    exit_velocity: Optional[float]
    launch_angle: Optional[float]
    inning: Optional[int]
    is_top_inning: Optional[bool]
    pitcher: Optional[str]
    pitch_type: Optional[str]
    event_time: Optional[str]


@dataclass
class DailyLeader:
    game_date: str
    batter: str
    team: str
    distance: int
    exit_velocity: Optional[float]
    launch_angle: Optional[float]
    inning: Optional[int]
    is_top_inning: Optional[bool]
    pitcher: Optional[str]
    pitch_type: Optional[str]
    event_time: Optional[str]
    tied: bool
    game_count: int
    updated_at: str


# ===================== DB =====================
def get_conn():
    if DB_URL and DB_URL.startswith("postgres"):
        import psycopg2
        return psycopg2.connect(DB_URL)
    return sqlite3.connect(SQLITE_PATH)


def init_db(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_leaders (
            game_date TEXT PRIMARY KEY,
            batter TEXT,
            team TEXT,
            distance INTEGER,
            exit_velocity REAL,
            launch_angle REAL,
            inning INTEGER,
            is_top_inning INTEGER,
            pitcher TEXT,
            pitch_type TEXT,
            event_time TEXT,
            tied INTEGER,
            game_count INTEGER,
            updated_at TEXT
        )
        """
    )
    conn.commit()


# ===================== MLB DATA =====================
def session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def fetch_schedule(s, target_date: str):
    params = {"sportId": SPORT_ID, "gameType": GAME_TYPE, "date": target_date}
    r = s.get(SCHEDULE_URL, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    games = []
    for d in data.get("dates", []):
        games.extend(d.get("games", []))
    return games


def fetch_live_feed(s, game_pk: int, game_link: str = None):
    """Fetch live feed using either game_pk or the actual game link from MLB API."""
    # Try using the actual game link first if provided
    if game_link:
        try:
            url = f"https://statsapi.mlb.com{game_link}/feed/live"
            r = s.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception:
            pass  # Fall back to game_pk method
    
    # Fall back to constructing URL from game_pk
    r = s.get(LIVE_FEED_URL.format(game_pk=game_pk), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def is_game_active_or_final(game_state: str) -> bool:
    """Always return True - fetch data for all games."""
    return True


def extract_home_runs(feed, game_pk: int, debug: bool = False):
    """
    Extract home runs from live feed with robust detection.
    """
    events = []
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])

    if debug:
        print(f"    [Debug] Total plays in feed: {len(plays)}")

    away_team = feed.get("gameData", {}).get("teams", {}).get("away", {})
    home_team = feed.get("gameData", {}).get("teams", {}).get("home", {})

    for play in plays:
        result = play.get("result", {}) or {}
        event_type = (result.get("eventType") or "").lower()
        event = (result.get("event") or "").lower()
        description = (result.get("description") or "").lower()

        # More robust home run detection
        is_home_run = (
            event_type == "home_run"
            or event == "home run"
            or "homered" in description
            or "home run" in description
            or "solo shot" in description  # Additional variation
            or "grand slam" in description
            or "two-run" in description and "homer" in description
            or "three-run" in description and "homer" in description
        )

        if not is_home_run:
            continue

        hit = play.get("hitData", {}) or {}
        distance = hit.get("totalDistance")

        # IMPORTANT: Some HRs might not have distance yet. Log them but still try to capture.
        if distance is None:
            if debug:
                print(f"      [HR found but no distance] {description}")
            continue

        matchup = play.get("matchup", {}) or {}
        about = play.get("about", {}) or {}

        batting_team = away_team if about.get("isTopInning") else home_team
        team_name = batting_team.get("abbreviation") or batting_team.get("name") or ""

        events.append(
            HomeRunEvent(
                game_date=feed.get("gameData", {}).get("datetime", {}).get("officialDate"),
                game_pk=game_pk,
                batter=matchup.get("batter", {}).get("fullName"),
                team=team_name,
                distance=int(distance),
                exit_velocity=hit.get("launchSpeed"),
                launch_angle=hit.get("launchAngle"),
                inning=about.get("inning"),
                is_top_inning=about.get("isTopInning"),
                pitcher=matchup.get("pitcher", {}).get("fullName"),
                pitch_type=play.get("pitchData", {}).get("pitchType") if play.get("pitchData") else None,
                event_time=about.get("endTime"),
            )
        )

    return events


def compute_leader(events, game_count: int):
    if not events:
        return None

    max_dist = max(e.distance for e in events)
    winners = [e for e in events if e.distance == max_dist]
    winner = winners[0]

    return DailyLeader(
        game_date=winner.game_date,
        batter=winner.batter,
        team=winner.team,
        distance=winner.distance,
        exit_velocity=winner.exit_velocity,
        launch_angle=winner.launch_angle,
        inning=winner.inning,
        is_top_inning=winner.is_top_inning,
        pitcher=winner.pitcher,
        pitch_type=winner.pitch_type,
        event_time=winner.event_time,
        tied=len(winners) > 1,
        game_count=game_count,
        updated_at=datetime.utcnow().isoformat(),
    )


# ===================== CORE PROCESSING =====================
def process_date(target_date: date, debug: bool = False):
    conn = get_conn()
    init_db(conn)
    s = session()

    date_str = target_date.isoformat()
    now_utc = datetime.utcnow().isoformat()
    now_la = get_current_time_la().isoformat()
    
    if debug:
        print(f"\n{'='*60}")
        print(f"TIME DEBUG:")
        print(f"  UTC now: {now_utc}")
        print(f"  LA now: {now_la}")
        print(f"  Target date: {date_str}")
        print(f"{'='*60}")
    
    games = fetch_schedule(s, date_str)

    if debug:
        print(f"\n{'='*60}")
        print(f"Processing: {date_str}")
        print(f"Total games fetched: {len(games)}")
        print(f"{'='*60}")

    if len(games) < MIN_GAMES_FOR_PROMO:
        if debug:
            print(f"⚠️  Promo inactive: only {len(games)} games (need {MIN_GAMES_FOR_PROMO})")
        return {
            "promo_active": False,
            "game_count": len(games),
            "leader": None,
            "date": date_str,
        }

    events = []
    games_fetched = 0

    for idx, g in enumerate(games, 1):
        game_pk = g.get("gamePk")
        game_status = g.get("status", {}).get("detailedState", "Unknown")
        game_teams = f"{g.get('teams', {}).get('away', {}).get('team', {}).get('name')} @ {g.get('teams', {}).get('home', {}).get('team', {}).get('name')}"
        game_link = g.get("link", "N/A")

        if not game_pk:
            if debug:
                print(f"  [{idx}/{len(games)}] Skipped - no game_pk")
            continue

        # Check if game is worth fetching
        if not is_game_active_or_final(game_status):
            if debug:
                print(f"  [{idx}/{len(games)}] Skipped (status: {game_status}) - {game_teams}")
            continue

        try:
            if debug:
                print(f"  [{idx}/{len(games)}] Fetching... {game_teams} ({game_status})")
                print(f"    PK: {game_pk}, Link: {game_link}")

            feed = fetch_live_feed(s, game_pk, game_link)
            game_events = extract_home_runs(feed, game_pk, debug=debug)
            events.extend(game_events)
            games_fetched += 1

            if debug:
                if game_events:
                    print(f"    ✓ Found {len(game_events)} HR(s)")
                else:
                    print(f"    • No HRs detected")

        except requests.exceptions.HTTPError as e:
            if debug:
                print(f"    ✗ HTTP Error: {e}")
            continue
        except Exception as e:
            if debug:
                print(f"    ✗ Unexpected error: {e}")
            continue

    if debug:
        print(f"\nResults:")
        print(f"  Games fetched: {games_fetched}/{len(games)}")
        print(f"  Total HRs found: {len(events)}")

    leader = compute_leader(events, len(games))

    if leader:
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_leaders
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                leader.game_date,
                leader.batter,
                leader.team,
                leader.distance,
                leader.exit_velocity,
                leader.launch_angle,
                leader.inning,
                int(leader.is_top_inning) if leader.is_top_inning is not None else None,
                leader.pitcher,
                leader.pitch_type,
                leader.event_time,
                int(leader.tied),
                leader.game_count,
                leader.updated_at,
            ),
        )
        conn.commit()
        if debug:
            print(f"\n✅ Saved leader: {leader.batter} ({leader.distance} ft)")
    else:
        if debug:
            print(f"\n⚠️  No HR leader found yet")

    return {
        "promo_active": True,
        "game_count": len(games),
        "leader": leader,
        "date": date_str,
    }


def update_data():
    return process_date(promo_today())


# ===================== FASTAPI =====================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/api/today")
def get_today():
    result = update_data()

    leader = result["leader"]
    if not leader:
        return {
            "data": None,
            "promo_active": result["promo_active"],
            "game_count": result["game_count"],
            "last_checked": datetime.utcnow().isoformat(),
            "promo_date": promo_today().isoformat(),
        }

    return {
        "data": {
            "game_date": leader.game_date,
            "batter": leader.batter,
            "team": leader.team,
            "distance": leader.distance,
            "exit_velocity": leader.exit_velocity,
            "launch_angle": leader.launch_angle,
            "inning": leader.inning,
            "is_top_inning": leader.is_top_inning,
            "pitcher": leader.pitcher,
            "pitch_type": leader.pitch_type,
            "event_time": leader.event_time,
            "tied": leader.tied,
            "game_count": leader.game_count,
            "updated_at": leader.updated_at,
        },
        "promo_active": result["promo_active"],
        "game_count": result["game_count"],
        "last_checked": datetime.utcnow().isoformat(),
        "promo_date": promo_today().isoformat(),
    }


@app.get("/api/history")
def get_history():
    conn = get_conn()
    init_db(conn)
    rows = conn.execute(
        """
        SELECT game_date, batter, team, distance, exit_velocity, launch_angle,
               inning, is_top_inning, pitcher, pitch_type, event_time, tied,
               game_count, updated_at
        FROM daily_leaders
        ORDER BY game_date DESC
        """
    ).fetchall()

    data = []
    for row in rows:
        data.append({
            "game_date": row[0],
            "batter": row[1],
            "team": row[2],
            "distance": row[3],
            "exit_velocity": row[4],
            "launch_angle": row[5],
            "inning": row[6],
            "is_top_inning": row[7],
            "pitcher": row[8],
            "pitch_type": row[9],
            "event_time": row[10],
            "tied": bool(row[11]),
            "game_count": row[12],
            "updated_at": row[13],
        })

    return {"data": data}


@app.post("/api/refresh")
def refresh():
    result = process_date(promo_today(), debug=True)
    leader = result["leader"]

    return {
        "updated": True,
        "promo_active": result["promo_active"],
        "game_count": result["game_count"],
        "leader": {
            "game_date": leader.game_date,
            "batter": leader.batter,
            "team": leader.team,
            "distance": leader.distance,
            "exit_velocity": leader.exit_velocity,
            "launch_angle": leader.launch_angle,
            "inning": leader.inning,
            "is_top_inning": leader.is_top_inning,
            "pitcher": leader.pitcher,
            "pitch_type": leader.pitch_type,
            "event_time": leader.event_time,
            "tied": leader.tied,
            "game_count": leader.game_count,
            "updated_at": leader.updated_at,
        } if leader else None
    }


@app.get("/", response_class=HTMLResponse)
def homepage():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Boomer's Sportsbook Longest Home Run Tracker</title>
        <style>
            :root {
                --bg: #0b1020;
                --panel: #121933;
                --panel-2: #192248;
                --text: #f5f7ff;
                --muted: #aeb8d6;
                --accent: #f4b400;
                --good: #18c37e;
                --border: rgba(255,255,255,0.08);
            }
            * { box-sizing: border-box; }
            body {
                margin: 0;
                font-family: Inter, Arial, Helvetica, sans-serif;
                background: linear-gradient(180deg, #09101d 0%, #0f1730 100%);
                color: var(--text);
            }
            .wrap {
                max-width: 1100px;
                margin: 0 auto;
                padding: 32px 20px 60px;
            }
            .hero {
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                gap: 20px;
                margin-bottom: 24px;
            }
            .hero-left {
                display: flex;
                align-items: center;
                gap: 20px;
            }
            .brand-logo {
                height: 80px;
                width: auto;
                display: block;
                filter: drop-shadow(0 6px 14px rgba(0,0,0,0.5));
            }
            .title {
                font-size: 34px;
                font-weight: 800;
                letter-spacing: -0.02em;
                margin: 0 0 10px;
            }
            .subtitle {
                color: var(--muted);
                margin: 0;
                max-width: 760px;
                line-height: 1.5;
            }
            .badge {
                display: inline-flex;
                align-items: center;
                gap: 8px;
                background: rgba(24,195,126,0.15);
                color: #8af0c2;
                border: 1px solid rgba(24,195,126,0.25);
                padding: 10px 14px;
                border-radius: 999px;
                font-size: 13px;
                font-weight: 700;
                white-space: nowrap;
            }
            .refresh-note {
                color: var(--muted);
                font-size: 12px;
                margin-top: 8px;
            }
            .grid {
                display: grid;
                grid-template-columns: 1.3fr 0.7fr;
                gap: 20px;
                margin-bottom: 20px;
            }
            .card {
                background: rgba(18,25,51,0.88);
                border: 1px solid var(--border);
                border-radius: 22px;
                padding: 22px;
                box-shadow: 0 12px 32px rgba(0,0,0,0.25);
                backdrop-filter: blur(10px);
            }
            .card h2 {
                margin: 0 0 14px;
                font-size: 18px;
            }
            .leader-header {
                display: flex;
                align-items: center;
                gap: 16px;
                margin-bottom: 12px;
            }
            .mascot {
                height: 90px;
                width: auto;
                display: block;
                flex-shrink: 0;
            }
            .leader-name {
                font-size: 32px;
                font-weight: 800;
                margin: 4px 0 8px;
            }
            .leader-meta {
                color: var(--muted);
                margin-bottom: 18px;
                font-size: 15px;
            }
            .distance {
                font-size: 58px;
                line-height: 1;
                font-weight: 900;
                color: var(--accent);
                margin-bottom: 24px;
            }
            .distance span {
                font-size: 22px;
                color: var(--text);
                margin-left: 6px;
            }
            .stats {
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 12px;
            }
            .stat {
                background: var(--panel-2);
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 14px;
            }
            .stat-label {
                font-size: 12px;
                color: var(--muted);
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 6px;
            }
            .stat-value {
                font-size: 19px;
                font-weight: 700;
            }
            .small-grid {
                display: grid;
                gap: 14px;
            }
            .small-card {
                background: var(--panel-2);
                border: 1px solid var(--border);
                border-radius: 18px;
                padding: 16px;
            }
            .small-card .label {
                font-size: 12px;
                color: var(--muted);
                text-transform: uppercase;
                letter-spacing: 0.08em;
                margin-bottom: 8px;
            }
            .small-card .value {
                font-size: 24px;
                font-weight: 800;
            }
            .table-card {
                background: rgba(18,25,51,0.88);
                border: 1px solid var(--border);
                border-radius: 22px;
                padding: 0;
                overflow: hidden;
                box-shadow: 0 12px 32px rgba(0,0,0,0.25);
            }
            .table-head {
                padding: 20px 22px;
                border-bottom: 1px solid var(--border);
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
            }
            .table-head h2 {
                margin: 0;
                font-size: 18px;
            }
            .table-wrap { overflow-x: auto; }
            table {
                width: 100%;
                border-collapse: collapse;
            }
            th, td {
                padding: 14px 18px;
                text-align: left;
                border-bottom: 1px solid var(--border);
                font-size: 14px;
            }
            th {
                color: var(--muted);
                font-weight: 700;
                text-transform: uppercase;
                font-size: 12px;
                letter-spacing: 0.08em;
                background: rgba(255,255,255,0.02);
            }
            tr:last-child td { border-bottom: 0; }
            .muted { color: var(--muted); }
            .pill {
                display: inline-block;
                padding: 5px 10px;
                border-radius: 999px;
                background: rgba(244,180,0,0.12);
                color: #ffd56a;
                border: 1px solid rgba(244,180,0,0.22);
                font-size: 12px;
                font-weight: 700;
            }
            .error {
                background: rgba(255,77,77,0.12);
                border: 1px solid rgba(255,77,77,0.25);
                color: #ffb0b0;
                border-radius: 16px;
                padding: 14px;
                margin-bottom: 18px;
            }
            @media (max-width: 900px) {
                .grid { grid-template-columns: 1fr; }
                .hero { flex-direction: column; }
                .hero-left { flex-direction: column; align-items: flex-start; }
                .leader-header { flex-direction: column; align-items: flex-start; }
            }
        </style>
    </head>
    <body>
        <div class="wrap">
            <div class="hero">
                <div class="hero-left">
                    <img src="/static/logo.png" alt="Boomer's Sportsbook" class="brand-logo" />
                    <div>
                        <h1 class="title">Longest Home Run Tracker</h1>
                        <p class="subtitle">Live tracker for the current longest home run of the day, powered by MLB game feeds. Promo eligibility requires at least five games on the slate.</p>
                        <div id="lastChecked" class="refresh-note">Checking for updates...</div>
                    </div>
                </div>
                <div id="promoBadge" class="badge">Checking slate...</div>
            </div>

            <div id="errorBox"></div>

            <div class="grid">
                <div class="card">
                    <h2>Current Daily Leader</h2>
                    <div class="leader-header">
                        <img src="/static/dog.png" alt="Boomer's mascot" class="mascot" />
                        <div>
                            <div id="leaderName" class="leader-name">Loading...</div>
                            <div id="leaderMeta" class="leader-meta">Please wait</div>
                        </div>
                    </div>
                    <div id="leaderDistance" class="distance">--<span>ft</span></div>
                    <div class="stats">
                        <div class="stat"><div class="stat-label">Exit Velocity</div><div id="ev" class="stat-value">--</div></div>
                        <div class="stat"><div class="stat-label">Launch Angle</div><div id="la" class="stat-value">--</div></div>
                        <div class="stat"><div class="stat-label">Pitcher</div><div id="pitcher" class="stat-value">--</div></div>
                        <div class="stat"><div class="stat-label">Updated</div><div id="updated" class="stat-value">--</div></div>
                    </div>
                </div>

                <div class="small-grid">
                    <div class="small-card">
                        <div class="label">Games on Slate</div>
                        <div id="gamesOnSlate" class="value">--</div>
                    </div>
                    <div class="small-card">
                        <div class="label">Promo Status</div>
                        <div id="promoStatus" class="value">--</div>
                    </div>
                    <div class="small-card">
                        <div class="label">Tie Status</div>
                        <div id="tieStatus" class="value">--</div>
                    </div>
                </div>
            </div>

            <div class="table-card">
                <div class="table-head">
                    <h2>Stored Leader History</h2>
                    <span class="muted">Tracked from site launch forward</span>
                </div>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Date</th>
                                <th>Player</th>
                                <th>Team</th>
                                <th>Distance</th>
                                <th>EV</th>
                                <th>LA</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody id="historyRows">
                            <tr><td colspan="7" class="muted">Loading history...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <script>
            function fmt(value, suffix = '') {
                return value === null || value === undefined || value === '' ? '--' : `${value}${suffix}`;
            }

            function mapRow(row) {
                return row || null;
            }

            function setError(message) {
                const box = document.getElementById('errorBox');
                box.innerHTML = message ? `<div class="error">${message}</div>` : '';
            }

            async function loadToday() {
                const res = await fetch('/api/today');
                const json = await res.json();
                const row = mapRow(json.data);

                document.getElementById('gamesOnSlate').textContent = fmt(json.game_count);
                document.getElementById('promoStatus').textContent = json.promo_active ? 'Active' : 'Inactive';
                document.getElementById('promoBadge').textContent = json.promo_active ? 'Promo Active (5+ games)' : 'Promo Inactive';
                document.getElementById('lastChecked').textContent = json.last_checked
                    ? `Last checked: ${new Date(json.last_checked).toLocaleTimeString()} • Promo date: ${json.promo_date || '--'}`
                    : 'Checking for updates...';

                if (!row) {
                    document.getElementById('leaderName').textContent = 'No leader yet';
                    document.getElementById('leaderMeta').textContent = json.promo_active
                        ? 'Waiting for first home run of the day'
                        : 'Fewer than 5 games on the slate';
                    document.getElementById('leaderDistance').innerHTML = `--<span>ft</span>`;
                    document.getElementById('ev').textContent = '--';
                    document.getElementById('la').textContent = '--';
                    document.getElementById('pitcher').textContent = '--';
                    document.getElementById('updated').textContent = '--';
                    document.getElementById('tieStatus').textContent = '--';
                    return null;
                }

                document.getElementById('leaderName').textContent = row.batter || 'Unknown';
                document.getElementById('leaderMeta').textContent = `${row.team || '--'} • ${row.game_date || '--'}`;
                document.getElementById('leaderDistance').innerHTML = `${fmt(row.distance)}<span>ft</span>`;
                document.getElementById('ev').textContent = fmt(row.exit_velocity, ' mph');
                document.getElementById('la').textContent = fmt(row.launch_angle, '°');
                document.getElementById('pitcher').textContent = row.pitcher || '--';
                document.getElementById('updated').textContent = row.updated_at ? new Date(row.updated_at).toLocaleTimeString() : '--';
                document.getElementById('tieStatus').textContent = row.tied ? 'Tied' : 'Clear';
                return row;
            }

            async function loadHistory() {
                const res = await fetch('/api/history');
                const json = await res.json();
                const rows = json.data || [];
                const tbody = document.getElementById('historyRows');

                if (!rows.length) {
                    tbody.innerHTML = '<tr><td colspan="7" class="muted">No stored history yet.</td></tr>';
                    return;
                }

                tbody.innerHTML = rows.map(raw => {
                    const row = mapRow(raw);
                    return `
                        <tr>
                            <td>${row.game_date || '--'}</td>
                            <td>${row.batter || '--'}</td>
                            <td>${row.team || '--'}</td>
                            <td>${fmt(row.distance, ' ft')}</td>
                            <td>${fmt(row.exit_velocity, ' mph')}</td>
                            <td>${fmt(row.launch_angle, '°')}</td>
                            <td>${row.tied ? '<span class="pill">Tie</span>' : '<span class="muted">Clear</span>'}</td>
                        </tr>
                    `;
                }).join('');
            }

            async function boot() {
                try {
                    setError('');
                    await loadToday();
                    await loadHistory();
                } catch (err) {
                    setError('Unable to load tracker data right now.');
                    console.error(err);
                }
            }

            boot();
            setInterval(boot, 30000);
        </script>
    </body>
    </html>
    """


# ===================== WORKER =====================
def run_worker():
    print("Starting worker...")
    while True:
        try:
            process_date(promo_today(), debug=True)
            print("Updated", datetime.now())
        except Exception as e:
            print("Error:", e)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()

    if args.worker:
        run_worker()
