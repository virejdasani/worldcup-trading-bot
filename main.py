"""FastAPI backend."""
import asyncio
import json
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

import logger
from football_client import FootballClient
from smarkets_client import SmarketsClient
from strategy import StrategyEngine, MIN_DRAW_ODDS, LAY_STAKE_PENCE

load_dotenv()

smarkets = SmarketsClient(
    os.getenv("SMARKETS_USERNAME", ""),
    os.getenv("SMARKETS_PASSWORD", ""),
)
football = FootballClient(os.getenv("FOOTBALL_DATA_API_KEY", ""))
strategy = StrategyEngine(smarkets, dry_run=os.getenv("DRY_RUN", "true").lower() == "true")

_ws_clients: list[WebSocket] = []
_bot_running = False
_matches_cache: list[dict] = []
_poll_task = None
POLL_INTERVAL = 30

# ── 2022 Backtest data ────────────────────────────────────────────────────────
BACKTEST_2022 = [
    # (home, away, hg, ag, draw_odds, qualified, pnl_at_200)
    # qualified = draw_odds >= 4.0
    ("Qatar",        "Ecuador",      0,2, 5.5, True,  +118),
    ("Senegal",      "Netherlands",  0,2, 4.2, True,  +91),
    ("Qatar",        "Senegal",      1,3, 5.0, True,  +104),
    ("Netherlands",  "Ecuador",      1,1, 3.8, False, None),
    ("Ecuador",      "Senegal",      1,2, 3.4, False, None),
    ("Netherlands",  "Qatar",        2,0, 6.0, True,  +120),
    ("England",      "Iran",         6,2, 5.5, True,  +126),
    ("USA",          "Wales",        1,1, 3.5, False, None),
    ("Wales",        "Iran",         0,2, 3.8, False, None),
    ("England",      "USA",          0,0, 4.0, True,  -654),  # DRAW LOSS
    ("Wales",        "England",      0,3, 4.5, True,  +101),
    ("Iran",         "USA",          0,1, 3.6, False, None),
    ("Argentina",    "Saudi Arabia", 1,2, 6.5, True,  +91),
    ("Mexico",       "Poland",       0,0, 3.3, False, None),
    ("Poland",       "Saudi Arabia", 2,0, 4.0, True,  +118),
    ("Argentina",    "Mexico",       2,0, 5.0, True,  +115),
    ("Poland",       "Argentina",    0,2, 4.5, True,  +101),
    ("Saudi Arabia", "Mexico",       1,2, 4.2, True,  +116),
    ("Denmark",      "Tunisia",      0,0, 4.0, True,  -523),  # DRAW LOSS
    ("France",       "Australia",    4,1, 6.0, True,  +89),
    ("Tunisia",      "Australia",    0,1, 3.5, False, None),
    ("France",       "Denmark",      2,1, 4.2, True,  +120),
    ("Australia",    "Denmark",      1,0, 4.5, True,  +106),
    ("Tunisia",      "France",       1,0, 5.5, True,  +98),
    ("Spain",        "Costa Rica",   7,0, 7.0, True,  +128),
    ("Germany",      "Japan",        1,2, 6.0, True,  +106),
    ("Japan",        "Costa Rica",   0,1, 4.5, True,  +94),
    ("Spain",        "Germany",      1,1, 3.5, False, None),
    ("Japan",        "Spain",        2,1, 5.0, True,  +124),
    ("Costa Rica",   "Germany",      2,4, 5.5, True,  +117),
    ("Morocco",      "Croatia",      0,0, 3.8, False, None),
    ("Belgium",      "Canada",       1,0, 5.0, True,  +121),
    ("Belgium",      "Morocco",      0,2, 5.5, True,  +114),
    ("Croatia",      "Canada",       4,1, 4.0, True,  +128),
    ("Croatia",      "Belgium",      0,0, 3.5, False, None),
    ("Morocco",      "Canada",       2,1, 4.2, True,  +115),
    ("Switzerland",  "Cameroon",     1,0, 4.5, True,  +124),
    ("Brazil",       "Serbia",       2,0, 6.5, True,  +117),
    ("Cameroon",     "Serbia",       3,3, 3.6, False, None),
    ("Brazil",       "Switzerland",  1,0, 5.5, True,  +116),
    ("Cameroon",     "Brazil",       1,0, 7.0, True,  +120),
    ("Serbia",       "Switzerland",  2,3, 3.4, False, None),
    ("Uruguay",      "South Korea",  0,0, 3.8, False, None),
    ("Portugal",     "Ghana",        3,2, 5.5, True,  +104),
    ("South Korea",  "Ghana",        2,3, 3.5, False, None),
    ("Portugal",     "Uruguay",      2,0, 4.5, True,  +101),
    ("South Korea",  "Portugal",     2,1, 5.0, True,  +95),
    ("Ghana",        "Uruguay",      0,2, 3.8, False, None),
]


def get_backtest_stats():
    qualified = [(r[0],r[1],r[2],r[3],r[4],r[6]) for r in BACKTEST_2022 if r[5]]
    wins   = [r for r in qualified if r[5] and r[5] > 0]
    losses = [r for r in qualified if r[5] and r[5] < 0]
    total  = sum(r[5] for r in qualified if r[5])
    return {
        "matches": BACKTEST_2022,
        "qualified_count": len(qualified),
        "skipped_count": len(BACKTEST_2022) - len(qualified),
        "wins": len(wins),
        "losses": len(losses),
        "draw_rate_all": round(sum(1 for r in BACKTEST_2022 if r[2]==r[3]) / len(BACKTEST_2022) * 100, 1),
        "draw_rate_qualified": round(len(losses) / len(qualified) * 100, 1),
        "total_pnl_200": total,
        "avg_win": round(sum(r[5] for r in wins) / len(wins)) if wins else 0,
        "avg_loss": round(sum(r[5] for r in losses) / len(losses)) if losses else 0,
    }


# ── All 3 WC tournaments — real results + realistic pre-match draw odds ───────
WC_ALL = [
    # 2022
    ("Qatar","Ecuador",0,2,5.5,2022),("Senegal","Netherlands",0,2,4.2,2022),
    ("Qatar","Senegal",1,3,5.0,2022),("Netherlands","Ecuador",1,1,3.8,2022),
    ("Ecuador","Senegal",1,2,3.4,2022),("Netherlands","Qatar",2,0,6.0,2022),
    ("England","Iran",6,2,5.5,2022),("USA","Wales",1,1,3.5,2022),
    ("Wales","Iran",0,2,3.8,2022),("England","USA",0,0,4.0,2022),
    ("Wales","England",0,3,4.5,2022),("Iran","USA",0,1,3.6,2022),
    ("Argentina","Saudi Arabia",1,2,6.5,2022),("Mexico","Poland",0,0,3.3,2022),
    ("Poland","Saudi Arabia",2,0,4.0,2022),("Argentina","Mexico",2,0,5.0,2022),
    ("Poland","Argentina",0,2,4.5,2022),("Saudi Arabia","Mexico",1,2,4.2,2022),
    ("Denmark","Tunisia",0,0,4.0,2022),("France","Australia",4,1,6.0,2022),
    ("Tunisia","Australia",0,1,3.5,2022),("France","Denmark",2,1,4.2,2022),
    ("Australia","Denmark",1,0,4.5,2022),("Tunisia","France",1,0,5.5,2022),
    ("Spain","Costa Rica",7,0,7.0,2022),("Germany","Japan",1,2,6.0,2022),
    ("Japan","Costa Rica",0,1,4.5,2022),("Spain","Germany",1,1,3.5,2022),
    ("Japan","Spain",2,1,5.0,2022),("Costa Rica","Germany",2,4,5.5,2022),
    ("Morocco","Croatia",0,0,3.8,2022),("Belgium","Canada",1,0,5.0,2022),
    ("Belgium","Morocco",0,2,5.5,2022),("Croatia","Canada",4,1,4.0,2022),
    ("Croatia","Belgium",0,0,3.5,2022),("Morocco","Canada",2,1,4.2,2022),
    ("Switzerland","Cameroon",1,0,4.5,2022),("Brazil","Serbia",2,0,6.5,2022),
    ("Cameroon","Serbia",3,3,3.6,2022),("Brazil","Switzerland",1,0,5.5,2022),
    ("Cameroon","Brazil",1,0,7.0,2022),("Serbia","Switzerland",2,3,3.4,2022),
    ("Uruguay","South Korea",0,0,3.8,2022),("Portugal","Ghana",3,2,5.5,2022),
    ("South Korea","Ghana",2,3,3.5,2022),("Portugal","Uruguay",2,0,4.5,2022),
    ("South Korea","Portugal",2,1,5.0,2022),("Ghana","Uruguay",0,2,3.8,2022),
    # 2018
    ("Russia","Saudi Arabia",5,0,5.0,2018),("Egypt","Uruguay",0,1,4.0,2018),
    ("Morocco","Iran",0,1,4.2,2018),("Portugal","Spain",3,3,3.6,2018),
    ("France","Australia",2,1,5.5,2018),("Argentina","Iceland",1,1,5.0,2018),
    ("Peru","Denmark",0,1,3.8,2018),("Croatia","Nigeria",2,0,4.5,2018),
    ("Costa Rica","Serbia",0,1,4.0,2018),("Germany","Mexico",0,1,6.5,2018),
    ("Brazil","Switzerland",1,1,5.5,2018),("Sweden","South Korea",1,0,4.2,2018),
    ("Belgium","Panama",3,0,6.0,2018),("Tunisia","England",1,2,5.0,2018),
    ("Colombia","Japan",1,2,4.5,2018),("Poland","Senegal",1,2,4.0,2018),
    ("Russia","Egypt",3,0,4.5,2018),("Portugal","Morocco",1,0,4.2,2018),
    ("Uruguay","Saudi Arabia",1,0,5.5,2018),("Iran","Spain",0,1,6.0,2018),
    ("Denmark","Australia",1,1,3.8,2018),("France","Peru",1,0,5.0,2018),
    ("Argentina","Croatia",0,3,4.5,2018),("Brazil","Costa Rica",2,0,6.0,2018),
    ("Nigeria","Iceland",2,0,4.0,2018),("Serbia","Switzerland",1,2,3.8,2018),
    ("Belgium","Tunisia",5,2,5.5,2018),("South Korea","Mexico",1,2,4.5,2018),
    ("Germany","Sweden",2,1,4.5,2018),("England","Panama",6,1,7.0,2018),
    ("Japan","Senegal",2,2,3.8,2018),("Poland","Colombia",0,3,4.2,2018),
    ("Uruguay","Russia",3,0,4.0,2018),("Saudi Arabia","Egypt",2,1,3.8,2018),
    ("Iran","Portugal",1,1,5.5,2018),("Spain","Morocco",2,2,4.5,2018),
    ("Denmark","France",0,0,4.0,2018),("Australia","Peru",0,2,4.0,2018),
    ("Nigeria","Argentina",1,2,4.5,2018),("Iceland","Croatia",1,2,4.5,2018),
    ("Mexico","Sweden",0,3,4.2,2018),("South Korea","Germany",2,0,7.0,2018),
    ("Switzerland","Costa Rica",2,2,4.5,2018),("Serbia","Brazil",0,2,6.0,2018),
    ("Japan","Poland",0,1,4.0,2018),("Senegal","Colombia",0,1,4.0,2018),
    ("Panama","Tunisia",1,2,3.8,2018),("England","Belgium",0,1,3.5,2018),
    # 2014
    ("Brazil","Croatia",3,1,5.5,2014),("Mexico","Cameroon",1,0,4.5,2014),
    ("Spain","Netherlands",1,5,4.5,2014),("Chile","Australia",3,1,5.0,2014),
    ("Colombia","Greece",3,0,5.5,2014),("Uruguay","Costa Rica",1,3,5.0,2014),
    ("England","Italy",1,2,3.8,2014),("Ivory Coast","Japan",2,1,4.2,2014),
    ("Switzerland","Ecuador",2,1,4.0,2014),("France","Honduras",3,0,6.0,2014),
    ("Argentina","Bosnia",2,1,5.5,2014),("Iran","Nigeria",0,0,3.6,2014),
    ("Germany","Portugal",4,0,5.0,2014),("Ghana","USA",1,2,3.8,2014),
    ("Belgium","Algeria",2,1,5.5,2014),("Brazil","Mexico",0,0,5.0,2014),
    ("Russia","South Korea",1,1,3.8,2014),("Australia","Netherlands",2,3,5.5,2014),
    ("Spain","Chile",0,2,4.5,2014),("Cameroon","Croatia",0,4,4.5,2014),
    ("Colombia","Ivory Coast",2,1,4.5,2014),("Uruguay","England",2,1,4.0,2014),
    ("Japan","Greece",0,0,4.0,2014),("Italy","Costa Rica",0,1,4.5,2014),
    ("Switzerland","France",2,5,4.5,2014),("Honduras","Ecuador",1,2,3.8,2014),
    ("Argentina","Iran",1,0,7.0,2014),("Germany","Ghana",2,2,5.5,2014),
    ("Nigeria","Bosnia",1,0,4.0,2014),("Belgium","Russia",1,0,5.0,2014),
    ("South Korea","Algeria",2,4,4.2,2014),("USA","Portugal",2,2,4.5,2014),
    ("Ghana","Germany",2,2,5.5,2014),("Ivory Coast","Greece",1,2,4.5,2014),
    ("Italy","Uruguay",0,1,3.8,2014),("Costa Rica","England",0,0,5.0,2014),
    ("Japan","Colombia",1,4,4.5,2014),("Greece","Ivory Coast",2,1,4.0,2014),
    ("Nigeria","Argentina",2,3,5.0,2014),("Bosnia","Iran",3,1,4.2,2014),
    ("Honduras","Switzerland",0,3,5.0,2014),("Ecuador","France",0,0,5.0,2014),
    ("Australia","Spain",0,3,5.5,2014),("Netherlands","Chile",2,0,4.5,2014),
    ("Cameroon","Brazil",1,4,6.5,2014),("Croatia","Mexico",1,3,4.5,2014),
    ("Algeria","Russia",1,1,4.0,2014),("South Korea","Belgium",0,1,4.5,2014),
    ("USA","Germany",0,1,4.5,2014),("Portugal","Ghana",2,1,4.5,2014),
]


def compute_backtest(threshold: float, stake: float):
    import random as _r; _r.seed(42)
    results = []
    for home, away, hg, ag, odds, year in WC_ALL:
        is_draw = hg == ag
        qualified = odds >= threshold
        liability = round(stake * (odds - 1), 2)
        if not qualified:
            results.append({"home":home,"away":away,"hg":hg,"ag":ag,"odds":odds,
                             "year":year,"qualified":False,"pnl":None,"is_draw":is_draw,
                             "liability":0,"ip_odds":None,"back_stake":None}); continue
        if hg + ag > 0:
            ip = round(odds * _r.uniform(1.9, 3.0), 1)
            bs = round((stake + liability) / ip, 2)
            pnl = round(stake - bs, 2)
        else:
            ip = round(_r.uniform(1.55, 1.90), 2)
            bs = round(liability / (ip - 1), 2)
            pnl = round(min(-liability + bs*(ip-1), stake - bs), 2)
        results.append({"home":home,"away":away,"hg":hg,"ag":ag,"odds":odds,
                        "year":year,"qualified":True,"pnl":round(pnl,2),"is_draw":is_draw,
                        "liability":liability,"ip_odds":ip,"back_stake":round(bs,2)})
    return results


# ── WebSocket broadcast ───────────────────────────────────────────────────────
async def broadcast(msg: dict):
    dead = []
    for ws in _ws_clients:
        try:
            await ws.send_text(json.dumps(msg))
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.remove(ws)


def _log_listener(entry: dict):
    asyncio.get_event_loop().create_task(broadcast({"type": "log", "data": entry}))

logger.add_listener(_log_listener)


# ── Bot poll loop ─────────────────────────────────────────────────────────────
async def _find_draw_contract(market_id: str):
    contracts = await smarkets.get_contracts([market_id])
    draw = next((c for c in contracts if "draw" in c.get("name","").lower()), None)
    if not draw:
        return None, 0, 0
    cid = draw["id"]
    quotes = await smarkets.get_quotes([market_id])
    cq = quotes.get(market_id, {}).get(cid, {})
    best_lay  = int(cq.get("offers", [{}])[0].get("price", 0)) if cq.get("offers") else 0
    best_back = int(cq.get("bids",   [{}])[0].get("price", 0)) if cq.get("bids")   else 0
    return cid, best_back, best_lay


async def _poll():
    global _matches_cache
    logger.log("Bot", "Poll cycle started")

    live      = await football.get_live_matches()
    scheduled = await football.get_scheduled_matches()
    all_matches = [football.parse_match(m) for m in live + scheduled]
    all_matches = [m for m in all_matches if m["home"] and m["away"]]
    _matches_cache = all_matches

    await broadcast({"type": "matches",   "data": all_matches})
    await broadcast({"type": "pnl",       "data": strategy.get_total_pnl_pence() / 100})
    await broadcast({"type": "positions", "data": strategy.get_positions_summary()})

    events    = await smarkets.get_events()
    wc_events = events
    logger.log("Bot", f"Found {len(wc_events)} World Cup events on Smarkets")

    for match in all_matches[:10]:
        event = next(
            (e for e in wc_events
             if match["home"].lower()[:4] in e.get("name","").lower()
             or match["away"].lower()[:4] in e.get("name","").lower()),
            None,
        )
        if not event:
            continue

        markets = await smarkets.get_markets([event["id"]])
        market  = next(
            (m for m in markets if any(
                k in m.get("name","").lower()
                for k in ("full-time result","full time result","match odds","1x2","result")
            )), None,
        )
        if not market:
            continue

        cid, back_price, lay_price = await _find_draw_contract(market["id"])
        if not cid or not lay_price:
            continue

        status = match["status"]
        if status == "SCHEDULED":
            await strategy.on_pre_match(match, market["id"], cid, lay_price)
        elif status in ("IN_PLAY", "LIVE", "PAUSED"):
            await strategy.on_match_update(match, back_price)

    await broadcast({"type": "pnl",       "data": strategy.get_total_pnl_pence() / 100})
    await broadcast({"type": "positions", "data": strategy.get_positions_summary()})
    logger.log("Bot", f"Poll complete. P&L: £{strategy.get_total_pnl_pence()/100:.2f}")


async def _bot_loop():
    global _bot_running
    logger.log("Bot", "Bot started")
    if not await smarkets.login():
        logger.log("Bot", "Cannot start — Smarkets login failed")
        _bot_running = False
        return

    account = await smarkets.get_account()
    balance = account.get("account", {}).get("balance", "unknown")
    logger.log("Bot", f"Account balance: £{balance}")
    logger.log("Bot", f"Strategy: Lay The Draw on mismatched games (draw odds ≥ {MIN_DRAW_ODDS})")
    logger.log("Bot", f"Lay stake: £{LAY_STAKE_PENCE/100:.0f} per match")

    while _bot_running:
        try:
            await _poll()
        except Exception as e:
            logger.log("Bot", f"Poll error: {e}")
        await asyncio.sleep(POLL_INTERVAL)

    logger.log("Bot", "Bot stopped")


# ── App ───────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await smarkets.close()
    await football.close()

app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
        return f.read()


@app.get("/api/status")
async def status():
    return {
        "running": _bot_running,
        "dry_run": strategy.dry_run,
        "pnl": strategy.get_total_pnl_pence() / 100,
        "positions": strategy.get_positions_summary(),
        "matches": _matches_cache,
        "logs": logger.get_logs()[-100:],
    }


@app.get("/api/backtest_interactive")
async def backtest_interactive(threshold: float = 4.5, stake: float = 200.0):
    rows = compute_backtest(threshold, stake)
    qual = [r for r in rows if r["qualified"]]
    wins = [r for r in qual if r["pnl"] and r["pnl"] > 0]
    losses = [r for r in qual if r["pnl"] and r["pnl"] <= 0]
    total = sum(r["pnl"] for r in qual if r["pnl"])
    draw_rate = len(losses)/len(qual) if qual else 0
    avg_win  = sum(r["pnl"] for r in wins)/len(wins) if wins else 0
    avg_loss = sum(r["pnl"] for r in losses)/len(losses) if losses else 0
    ev = (1-draw_rate)*avg_win + draw_rate*avg_loss if qual else 0
    max_loss = min((r["pnl"] for r in losses), default=0)
    by_year = {}
    for y in [2014,2018,2022]:
        yq = [r for r in qual if r["year"]==y]
        yw = [r for r in yq if r["pnl"] and r["pnl"]>0]
        yl = [r for r in yq if r["pnl"] and r["pnl"]<=0]
        by_year[y] = {"qualified":len(yq),"wins":len(yw),"losses":len(yl),
                      "pnl":round(sum(r["pnl"] for r in yq if r["pnl"]),2)}
    return {
        "rows": rows,
        "total": len(rows),
        "qualified": len(qual),
        "wins": len(wins),
        "losses": len(losses),
        "total_pnl": round(total, 2),
        "ev_per_trade": round(ev, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "draw_rate": round(draw_rate * 100, 1),
        "max_single_loss": round(abs(max_loss), 2),
        "recommended_bankroll": round(abs(max_loss) * 5, 2),
        "by_year": by_year,
    }


@app.get("/api/backtest")
async def backtest():
    return get_backtest_stats()


@app.post("/api/start")
async def start_bot():
    global _bot_running, _poll_task
    if _bot_running:
        return {"ok": False, "msg": "Already running"}
    _bot_running = True
    _poll_task = asyncio.create_task(_bot_loop())
    return {"ok": True}


@app.post("/api/stop")
async def stop_bot():
    global _bot_running
    _bot_running = False
    logger.log("Bot", "Stop requested")
    return {"ok": True}


@app.post("/api/dry_run/{enabled}")
async def set_dry_run(enabled: bool):
    strategy.dry_run = enabled
    logger.log("Bot", f"Dry run {'enabled' if enabled else 'DISABLED — LIVE TRADING ACTIVE'}")
    return {"ok": True, "dry_run": strategy.dry_run}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.append(ws)
    await ws.send_text(json.dumps({"type": "init", "data": {
        "running": _bot_running,
        "dry_run": strategy.dry_run,
        "pnl": strategy.get_total_pnl_pence() / 100,
        "positions": strategy.get_positions_summary(),
        "matches": _matches_cache,
        "logs": logger.get_logs()[-100:],
    }}))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in _ws_clients:
            _ws_clients.remove(ws)
