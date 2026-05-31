"""football-data.org client for live World Cup scores."""
import httpx
from logger import log

BASE = "https://api.football-data.org/v4"
# 2026 FIFA World Cup competition ID (will be confirmed closer to tournament)
WC_COMPETITION = "WC"


class FootballClient:
    def __init__(self, api_key: str):
        self._client = httpx.AsyncClient(
            headers={"X-Auth-Token": api_key},
            timeout=10,
        )

    async def get_live_matches(self) -> list[dict]:
        log("Football", f"Fetching live World Cup matches from football-data.org")
        r = await self._client.get(f"{BASE}/competitions/{WC_COMPETITION}/matches", params={
            "status": "LIVE,IN_PLAY,PAUSED",
        })
        if r.status_code == 200:
            matches = r.json().get("matches", [])
            log("Football", f"Got {len(matches)} live matches")
            return matches
        log("Football", f"Live matches failed: {r.status_code} {r.text}")
        return []

    async def get_scheduled_matches(self) -> list[dict]:
        log("Football", "Fetching scheduled World Cup matches")
        r = await self._client.get(f"{BASE}/competitions/{WC_COMPETITION}/matches", params={
            "status": "SCHEDULED,TIMED",
        })
        if r.status_code == 200:
            matches = r.json().get("matches", [])
            log("Football", f"Got {len(matches)} scheduled matches")
            return matches
        log("Football", f"Scheduled matches failed: {r.status_code} {r.text}")
        return []

    async def get_all_matches(self) -> list[dict]:
        log("Football", "Fetching all World Cup matches")
        r = await self._client.get(f"{BASE}/competitions/{WC_COMPETITION}/matches")
        if r.status_code == 200:
            matches = r.json().get("matches", [])
            log("Football", f"Got {len(matches)} total matches")
            return matches
        log("Football", f"All matches failed: {r.status_code} {r.text}")
        return []

    def parse_match(self, m: dict) -> dict:
        """Normalise a match into a flat dict for the UI and strategy."""
        score = m.get("score", {})
        full = score.get("fullTime", {})
        half = score.get("halfTime", {})
        return {
            "id": m["id"],
            "status": m["status"],
            "minute": m.get("minute"),
            "home": m["homeTeam"].get("name"),
            "away": m["awayTeam"].get("name"),
            "home_score": full.get("home") or half.get("home") or 0,
            "away_score": full.get("away") or half.get("away") or 0,
            "stage": m.get("stage", ""),
            "utc_date": m.get("utcDate", ""),
        }

    async def close(self):
        await self._client.aclose()
