"""
Strategy B: Lay The Draw (LTD) — Mismatched Games Only

Rules:
  1. Pre-match: LAY the draw when draw odds >= 4.0 (clear favourite exists)
  2. Goal scored at 45+ min: BACK the draw at spiked odds → lock profit
  3. Still 0-0 at 70 min: BACK the draw at shortened odds → limit loss
  4. Skip evenly-matched games (draw odds < 4.0)

Why it works:
  - In mismatched games, only 6% end in draws (vs 21% overall)
  - Goals spike draw odds 2-3x, letting us back at a profit
  - Exchange (no bookmaker margin) keeps the edge
"""
from dataclasses import dataclass, field
from typing import Optional
from logger import log

# ── Config ────────────────────────────────────────────────────────────────────
MIN_DRAW_ODDS   = 4.5    # Only trade when draw odds >= this (mismatched game)
LAY_STAKE_PENCE = 20000  # £200 lay stake per match (liability = stake × (odds-1))
GOAL_TRIGGER_MIN = 45    # Minimum match minute to trade out after goal
SCORELESS_MIN    = 70    # Trade out if still 0-0 at this minute


@dataclass
class Position:
    match_id: int
    market_id: str
    draw_contract_id: str
    home: str
    away: str
    pre_draw_odds: float       # odds we laid at pre-match

    lay_order_id: Optional[str] = None
    back_order_id: Optional[str] = None
    back_odds: Optional[float] = None
    back_stake_pence: int = 0

    state: str = "laid"        # laid → traded_out
    pnl_pence: int = 0
    goal_triggered: bool = False

    @property
    def liability_pence(self) -> int:
        return int(LAY_STAKE_PENCE * (self.pre_draw_odds - 1))

    @property
    def label(self) -> str:
        return f"{self.home} vs {self.away}"


class StrategyEngine:
    def __init__(self, smarkets, dry_run: bool = True):
        self.smarkets = smarkets
        self.dry_run = dry_run
        self.positions: dict[int, Position] = {}

    async def on_pre_match(self, match: dict, market_id: str,
                           draw_contract_id: str, draw_lay_price: int):
        """Called pre-match. Lay the draw if odds qualify."""
        mid = match["id"]
        if mid in self.positions:
            return

        odds = draw_lay_price / 100
        if odds < MIN_DRAW_ODDS:
            log("Strategy", f"{match['home']} vs {match['away']}: draw odds "
                            f"{odds:.2f} < {MIN_DRAW_ODDS} — SKIP (even match)")
            return

        log("Strategy", f"✓ QUALIFYING: {match['home']} vs {match['away']} "
                        f"draw odds {odds:.2f} ≥ {MIN_DRAW_ODDS} — laying draw "
                        f"£{LAY_STAKE_PENCE/100:.0f} (liability £{LAY_STAKE_PENCE*(odds-1)/100:.0f})")

        pos = Position(
            match_id=mid, market_id=market_id,
            draw_contract_id=draw_contract_id,
            home=match["home"], away=match["away"],
            pre_draw_odds=odds,
        )
        self.positions[mid] = pos

        if self.dry_run:
            log("Strategy", f"[DRY RUN] Lay order not placed")
            return

        result = await self.smarkets.place_order(
            market_id, draw_contract_id, "sell",
            LAY_STAKE_PENCE, draw_lay_price
        )
        if result:
            orders = result.get("orders", [result])
            pos.lay_order_id = orders[0].get("id") if orders else None
        else:
            log("Strategy", "Lay order failed — removing position")
            del self.positions[mid]

    async def on_match_update(self, match: dict, draw_back_price: int):
        """Called each poll during live match. Trade out when triggered."""
        mid = match["id"]
        pos = self.positions.get(mid)
        if not pos or pos.state != "laid" or pos.goal_triggered:
            return

        minute = match.get("minute") or 0
        scored = (match["home_score"] + match["away_score"]) > 0
        status = match["status"]

        if status not in ("IN_PLAY", "LIVE", "PAUSED"):
            return

        if scored and minute >= GOAL_TRIGGER_MIN:
            log("Strategy", f"⚽ GOAL at {minute}' — {pos.label}: draw odds "
                            f"spiked, backing to lock profit")
            await self._trade_out(pos, draw_back_price, reason="goal")

        elif not scored and minute >= SCORELESS_MIN:
            log("Strategy", f"⏱ 0-0 at {minute}' — {pos.label}: draw odds "
                            f"shortened, backing to limit loss")
            await self._trade_out(pos, draw_back_price, reason="0-0")

    async def _trade_out(self, pos: Position, back_price_int: int, reason: str):
        pos.goal_triggered = True
        back_odds = back_price_int / 100
        liability = pos.liability_pence

        # Back stake to equalise both outcomes
        # if_draw     = -liability + back_stake*(back_odds-1)
        # if_not_draw = LAY_STAKE_PENCE - back_stake
        # Equal when: back_stake = (LAY_STAKE_PENCE + liability) / back_odds
        if back_odds <= 1:
            log("Strategy", f"Back odds {back_odds} too low, skipping trade-out")
            return

        back_stake = int((LAY_STAKE_PENCE + liability) / back_odds)
        if_not_draw = LAY_STAKE_PENCE - back_stake
        if_draw     = -liability + back_stake * (back_odds - 1)
        locked = int(min(if_draw, if_not_draw))

        log("Strategy", f"Trade-out ({reason}): back draw @ {back_odds:.2f} "
                        f"stake £{back_stake/100:.2f} → locks "
                        f"{'profit' if locked > 0 else 'loss'} £{abs(locked)/100:.2f}")

        pos.back_odds = back_odds
        pos.back_stake_pence = back_stake
        pos.pnl_pence = locked
        pos.state = "traded_out"

        if self.dry_run:
            log("Strategy", "[DRY RUN] Back order not placed")
            return

        result = await self.smarkets.place_order(
            pos.market_id, pos.draw_contract_id, "buy",
            back_stake, back_price_int
        )
        if result:
            orders = result.get("orders", [result])
            pos.back_order_id = orders[0].get("id") if orders else None

    def get_total_pnl_pence(self) -> int:
        return sum(p.pnl_pence for p in self.positions.values())

    def get_positions_summary(self) -> list[dict]:
        return [
            {
                "match": p.label,
                "state": p.state,
                "lay_odds": f"{p.pre_draw_odds:.2f}",
                "back_odds": f"{p.back_odds:.2f}" if p.back_odds else "—",
                "liability": f"£{p.liability_pence/100:.0f}",
                "pnl": f"£{p.pnl_pence/100:.2f}",
                "pnl_raw": p.pnl_pence,
            }
            for p in self.positions.values()
        ]
