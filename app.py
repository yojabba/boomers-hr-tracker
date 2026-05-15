import os
import sqlite3
import time
import threading
import argparse
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
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
POLL_SECONDS = 60          # Background refresh cadence
REQUEST_TIMEOUT = 15
MIN_GAMES_FOR_PROMO = 5
USER_AGENT = "Mozilla/5.0 (BoomersBigFliesTracker/LiveOnly)"

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

# Nevada is on Pacific Time (same as LA)
PROMO_TZ = ZoneInfo("America/Los_Angeles")

DB_URL = os.getenv("DATABASE_URL")
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", "longest_hr_tracker.sqlite3"))


def promo_today() -> date:
    """Get today's date in Nevada/Pacific time, with safe fallback."""
    try:
        return datetime.now(PROMO_TZ).date()
    except Exception:
        from datetime import timedelta
        # Fallback: assume UTC minus 7 (PDT). Close enough for fallback only.
        return (datetime.utcnow() - timedelta(hours=7)).date()


def now_pacific() -> datetime:
    try:
        return datetime.now(PROMO_TZ)
    except Exception:
        from datetime import timedelta
        return datetime.utcnow() - timedelta(hours=7)


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
_db_lock = threading.Lock()


def get_conn():
    if DB_URL and DB_URL.startswith("postgres"):
        import psycopg2
        return psycopg2.connect(DB_URL)
    conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_status (
            game_date TEXT PRIMARY KEY,
            game_count INTEGER,
            promo_active INTEGER,
            last_checked TEXT
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


def fetch_live_feed(s, game_pk: int):
    r = s.get(LIVE_FEED_URL.format(game_pk=game_pk), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _pitch_type_str(pt):
    if isinstance(pt, dict):
        return pt.get("description") or pt.get("code")
    return pt


def extract_home_runs(feed, game_pk: int):
    """Extract HRs from a live feed."""
    events = []
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])

    away_team = feed.get("gameData", {}).get("teams", {}).get("away", {})
    home_team = feed.get("gameData", {}).get("teams", {}).get("home", {})
    official_date = feed.get("gameData", {}).get("datetime", {}).get("officialDate")

    for play in plays:
        result = play.get("result", {}) or {}
        event_type = (result.get("eventType") or "").lower()
        event_name = (result.get("event") or "").lower()

        # Rely on the structured event field — most reliable
        if event_type != "home_run" and event_name != "home run":
            continue

        hit = play.get("hitData", {}) or {}
        distance = hit.get("totalDistance")

        # Statcast distance not yet available — skip for now, next poll will catch it
        if distance is None:
            continue

        try:
            distance = int(float(distance))
        except (TypeError, ValueError):
            continue

        matchup = play.get("matchup", {}) or {}
        about = play.get("about", {}) or {}

        batting_team = away_team if about.get("isTopInning") else home_team
        team_name = batting_team.get("abbreviation") or batting_team.get("name") or ""

        events.append(
            HomeRunEvent(
                game_date=official_date,
                game_pk=game_pk,
                batter=(matchup.get("batter") or {}).get("fullName"),
                team=team_name,
                distance=distance,
                exit_velocity=hit.get("launchSpeed"),
                launch_angle=hit.get("launchAngle"),
                inning=about.get("inning"),
                is_top_inning=about.get("isTopInning"),
                pitcher=(matchup.get("pitcher") or {}).get("fullName"),
                pitch_type=_pitch_type_str((play.get("pitchData") or {}).get("pitchType")),
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
        updated_at=datetime.now(timezone.utc).isoformat(),
    )


# ===================== CORE PROCESSING =====================
def process_date(target_date: date, debug: bool = False):
    s = session()
    date_str = target_date.isoformat()

    if debug:
        print(f"[{now_pacific().isoformat()}] Fetching schedule for {date_str}")

    try:
        games = fetch_schedule(s, date_str)
    except Exception as e:
        print(f"[updater] schedule fetch failed: {e}")
        return {
            "promo_active": False,
            "game_count": 0,
            "leader": None,
            "date": date_str,
        }

    if debug:
        print(f"[updater] {len(games)} games on slate")

    promo_active = len(games) >= MIN_GAMES_FOR_PROMO

    events = []
    if promo_active:
        for g in games:
            game_pk = g.get("gamePk")
            if not game_pk:
                continue
            try:
                feed = fetch_live_feed(s, game_pk)
                events.extend(extract_home_runs(feed, game_pk))
            except requests.exceptions.HTTPError as e:
                if debug:
                    print(f"  HTTP error on game {game_pk}: {e}")
                continue
            except Exception as e:
                if debug:
                    print(f"  error on game {game_pk}: {e}")
                continue

    if debug:
        print(f"[updater] {len(events)} HRs found")

    leader = compute_leader(events, len(games)) if events else None

    # Persist
    with _db_lock:
        conn = get_conn()
        init_db(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_status
            (game_date, game_count, promo_active, last_checked)
            VALUES (?, ?, ?, ?)
            """,
            (
                date_str,
                len(games),
                int(promo_active),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
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
        conn.close()

    return {
        "promo_active": promo_active,
        "game_count": len(games),
        "leader": leader,
        "date": date_str,
    }


# ===================== BACKGROUND UPDATER =====================
_updater_thread: Optional[threading.Thread] = None
_updater_stop = threading.Event()


def _updater_loop():
    """Background polling loop. Runs inside the web service."""
    print("[updater] background thread started")
    while not _updater_stop.is_set():
        try:
            process_date(promo_today(), debug=True)
        except Exception as e:
            print(f"[updater] error: {e}")
        _updater_stop.wait(POLL_SECONDS)


def start_updater():
    global _updater_thread
    if _updater_thread and _updater_thread.is_alive():
        return
    _updater_thread = threading.Thread(target=_updater_loop, daemon=True)
    _updater_thread.start()


# ===================== FASTAPI =====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    conn = get_conn()
    init_db(conn)
    conn.close()
    start_updater()
    yield
    # Shutdown
    _updater_stop.set()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


def _read_today_from_db():
    """Cheap read — no API calls."""
    today_str = promo_today().isoformat()
    with _db_lock:
        conn = get_conn()
        init_db(conn)
        status = conn.execute(
            "SELECT game_count, promo_active, last_checked FROM daily_status WHERE game_date = ?",
            (today_str,),
        ).fetchone()
        leader = conn.execute(
            """
            SELECT game_date, batter, team, distance, exit_velocity, launch_angle,
                   inning, is_top_inning, pitcher, pitch_type, event_time, tied,
                   game_count, updated_at
            FROM daily_leaders WHERE game_date = ?
            """,
            (today_str,),
        ).fetchone()
        conn.close()

    game_count = status[0] if status else 0
    promo_active = bool(status[1]) if status else False
    last_checked = status[2] if status else None

    leader_dict = None
    if leader:
        leader_dict = {
            "game_date": leader[0],
            "batter": leader[1],
            "team": leader[2],
            "distance": leader[3],
            "exit_velocity": leader[4],
            "launch_angle": leader[5],
            "inning": leader[6],
            "is_top_inning": bool(leader[7]) if leader[7] is not None else None,
            "pitcher": leader[8],
            "pitch_type": leader[9],
            "event_time": leader[10],
            "tied": bool(leader[11]),
            "game_count": leader[12],
            "updated_at": leader[13],
        }

    return {
        "data": leader_dict,
        "promo_active": promo_active,
        "game_count": game_count,
        "last_checked": last_checked or datetime.now(timezone.utc).isoformat(),
        "promo_date": today_str,
    }


@app.get("/api/today")
def get_today():
    return _read_today_from_db()


@app.get("/api/history")
def get_history():
    with _db_lock:
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
        conn.close()

    data = [{
        "game_date": r[0],
        "batter": r[1],
        "team": r[2],
        "distance": r[3],
        "exit_velocity": r[4],
        "launch_angle": r[5],
        "inning": r[6],
        "is_top_inning": bool(r[7]) if r[7] is not None else None,
        "pitcher": r[8],
        "pitch_type": r[9],
        "event_time": r[10],
        "tied": bool(r[11]),
        "game_count": r[12],
        "updated_at": r[13],
    } for r in rows]

    return {"data": data}


@app.post("/api/refresh")
def refresh():
    """Force an immediate refresh (synchronous)."""
    result = process_date(promo_today(), debug=True)
    leader = result["leader"]
    return {
        "updated": True,
        "promo_active": result["promo_active"],
        "game_count": result["game_count"],
        "leader": asdict(leader) if leader else None,
    }


@app.get("/api/health")
def health():
    return {"status": "ok", "time_pacific": now_pacific().isoformat()}


@app.get("/", response_class=HTMLResponse)
def homepage():
    return HOMEPAGE_HTML


# ===================== HTML =====================
HOMEPAGE_HTML = """
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
        .wrap { max-width: 1100px; margin: 0 auto; padding: 32px 20px 60px; }
        .hero {
            display: flex; justify-content: space-between; align-items: flex-start;
            gap: 20px; margin-bottom: 24px;
        }
        .hero-left { display: flex; align-items: center; gap: 20px; }
        .brand-logo { height: 80px; width: auto; display: block; filter: drop-shadow(0 6px 14px rgba(0,0,0,0.5)); }
        .title { font-size: 34px; font-weight: 800; letter-spacing: -0.02em; margin: 0 0 10px; }
        .subtitle { color: var(--muted); margin: 0; max-width: 760px; line-height: 1.5; }
        .badge {
            display: inline-flex; align-items: center; gap: 8px;
            background: rgba(24,195,126,0.15); color: #8af0c2;
            border: 1px solid rgba(24,195,126,0.25); padding: 10px 14px;
            border-radius: 999px; font-size: 13px; font-weight: 700; white-space: nowrap;
        }
        .refresh-note { color: var(--muted); font-size: 12px; margin-top: 8px; }
        .grid { display: grid; grid-template-columns: 1.3fr 0.7fr; gap: 20px; margin-bottom: 20px; }
        .card {
            background: rgba(18,25,51,0.88); border: 1px solid var(--border);
            border-radius: 22px; padding: 22px;
            box-shadow: 0 12px 32px rgba(0,0,0,0.25); backdrop-filter: blur(10px);
        }
        .card h2 { margin: 0 0 14px; font-size: 18px; }
        .leader-header { display: flex; align-items: center; gap: 16px; margin-bottom: 12px; }
        .mascot { height: 90px; width: auto; display: block; flex-shrink: 0; }
        .leader-name { font-size: 32px; font-weight: 800; margin: 4px 0 8px; }
        .leader-meta { color: var(--muted); margin-bottom: 18px; font-size: 15px; }
        .distance { font-size: 58px; line-height: 1; font-weight: 900; color: var(--accent); margin-bottom: 24px; }
        .distance span { font-size: 22px; color: var(--text); margin-left: 6px; }
        .stats { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
        .stat { background: var(--panel-2); border: 1px solid var(--border); border-radius: 16px; padding: 14px; }
        .stat-label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 6px; }
        .stat-value { font-size: 19px; font-weight: 700; }
        .small-grid { display: grid; gap: 14px; }
        .small-card { background: var(--panel-2); border: 1px solid var(--border); border-radius: 18px; padding: 16px; }
        .small-card .label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; }
        .small-card .value { font-size: 24px; font-weight: 800; }
        .table-card { background: rgba(18,25,51,0.88); border: 1px solid var(--border); border-radius: 22px; padding: 0; overflow: hidden; box-shadow: 0 12px 32px rgba(0,0,0,0.25); }
        .table-head { padding: 20px 22px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; gap: 12px; }
        .table-head h2 { margin: 0; font-size: 18px; }
        .table-wrap { overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 14px 18px; text-align: left; border-bottom: 1px solid var(--border); font-size: 14px; }
        th { color: var(--muted); font-weight: 700; text-transform: uppercase; font-size: 12px; letter-spacing: 0.08em; background: rgba(255,255,255,0.02); }
        tr:last-child td { border-bottom: 0; }
        .muted { color: var(--muted); }
        .pill { display: inline-block; padding: 5px 10px; border-radius: 999px; background: rgba(244,180,0,0.12); color: #ffd56a; border: 1px solid rgba(244,180,0,0.22); font-size: 12px; font-weight: 700; }
        .error { background: rgba(255,77,77,0.12); border: 1px solid rgba(255,77,77,0.25); color: #ffb0b0; border-radius: 16px; padding: 14px; margin-bottom: 18px; }
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
                    <p class="subtitle">Bet a player to hit a home run — if his shot is the longest of the day across MLB, you get 3x your stake in bonus bets. Promo requires 5+ games on the slate.</p>
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
                <div class="small-card"><div class="label">Games on Slate</div><div id="gamesOnSlate" class="value">--</div></div>
                <div class="small-card"><div class="label">Promo Status</div><div id="promoStatus" class="value">--</div></div>
                <div class="small-card"><div class="label">Tie Status</div><div id="tieStatus" class="value">--</div></div>
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
                        <tr><th>Date</th><th>Player</th><th>Team</th><th>Distance</th><th>EV</th><th>LA</th><th>Status</th></tr>
                    </thead>
                    <tbody id="historyRows"><tr><td colspan="7" class="muted">Loading history...</td></tr></tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        function fmt(value, suffix = '') {
            return value === null || value === undefined || value === '' ? '--' : `${value}${suffix}`;
        }
        function setError(message) {
            const box = document.getElementById('errorBox');
            box.innerHTML = message ? `<div class="error">${message}</div>` : '';
        }
        async function loadToday() {
            const res = await fetch('/api/today');
            const json = await res.json();
            const row = json.data;

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
                return;
            }

            document.getElementById('leaderName').textContent = row.batter || 'Unknown';
            document.getElementById('leaderMeta').textContent = `${row.team || '--'} • ${row.game_date || '--'}`;
            document.getElementById('leaderDistance').innerHTML = `${fmt(row.distance)}<span>ft</span>`;
            document.getElementById('ev').textContent = fmt(row.exit_velocity, ' mph');
            document.getElementById('la').textContent = fmt(row.launch_angle, '°');
            document.getElementById('pitcher').textContent = row.pitcher || '--';
            document.getElementById('updated').textContent = row.updated_at ? new Date(row.updated_at).toLocaleTimeString() : '--';
            document.getElementById('tieStatus').textContent = row.tied ? 'Tied' : 'Clear';
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
            tbody.innerHTML = rows.map(row => `
                <tr>
                    <td>${row.game_date || '--'}</td>
                    <td>${row.batter || '--'}</td>
                    <td>${row.team || '--'}</td>
                    <td>${fmt(row.distance, ' ft')}</td>
                    <td>${fmt(row.exit_velocity, ' mph')}</td>
                    <td>${fmt(row.launch_angle, '°')}</td>
                    <td>${row.tied ? '<span class="pill">Tie</span>' : '<span class="muted">Clear</span>'}</td>
                </tr>
            `).join('');
        }

        async function boot() {
            try { setError(''); await loadToday(); await loadHistory(); }
            catch (err) { setError('Unable to load tracker data right now.'); console.error(err); }
        }
        boot();
        setInterval(boot, 30000);
    </script>
</body>
</html>
"""


# ===================== ENTRY =====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true", help="Run only the polling worker")
    parser.add_argument("--once", action="store_true", help="Run one fetch and exit")
    args = parser.parse_args()

    if args.once:
        print(process_date(promo_today(), debug=True))
    elif args.worker:
        # Legacy mode; the web service now runs its own updater.
        start_updater()
        _updater_thread.join()
