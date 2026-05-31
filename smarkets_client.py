"""Smarkets HTTP API client."""
import httpx
from typing import Optional
from logger import log

BASE = "https://api.smarkets.com/v3"


class SmarketsClient:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.token: Optional[str] = None
        self._client = httpx.AsyncClient(timeout=10)

    def _headers(self) -> dict:
        return {"Session-Token": self.token} if self.token else {}

    async def login(self) -> bool:
        log("Smarkets", f"Logging in as {self.username}")
        r = await self._client.post(f"{BASE}/sessions/", json={
            "username": self.username,
            "password": self.password,
        })
        if r.status_code in (200, 201):
            self.token = r.json()["token"]
            log("Smarkets", "Login successful, session token acquired")
            return True
        log("Smarkets", f"Login failed: {r.status_code} {r.text}")
        return False

    async def get_events(self) -> list:
        seen, results = set(), []
        for state in ("upcoming", "live"):
            log("Smarkets", f"Fetching World Cup events (state={state})")
            r = await self._client.get(f"{BASE}/events/", headers=self._headers(), params={
                "type": "football_match",
                "state": state,
                "name": "world cup",
                "limit": 200,
            })
            if r.status_code != 200:
                log("Smarkets", f"get_events failed ({state}): {r.status_code} {r.text[:200]}")
                continue
            for e in r.json().get("events", []):
                if "world-cup" in e.get("full_slug", "") and e["id"] not in seen:
                    seen.add(e["id"])
                    results.append(e)
        log("Smarkets", f"Got {len(results)} World Cup events total")
        return results

    async def get_markets(self, event_ids: list) -> list:
        ids = ",".join(event_ids)
        log("Smarkets", f"Fetching markets for events: {ids}")
        r = await self._client.get(f"{BASE}/events/{ids}/markets/", headers=self._headers())
        if r.status_code != 200:
            log("Smarkets", f"get_markets failed: {r.status_code}")
            return []
        markets = r.json().get("markets", [])
        log("Smarkets", f"Got {len(markets)} markets")
        return markets

    async def get_contracts(self, market_ids: list) -> list:
        ids = ",".join(market_ids)
        log("Smarkets", f"Fetching contracts for markets: {ids}")
        r = await self._client.get(f"{BASE}/markets/{ids}/contracts/", headers=self._headers())
        if r.status_code != 200:
            log("Smarkets", f"get_contracts failed: {r.status_code}")
            return []
        return r.json().get("contracts", [])

    async def get_quotes(self, market_ids: list) -> dict:
        ids = ",".join(market_ids)
        log("Smarkets", f"Fetching quotes for markets: {ids}")
        r = await self._client.get(f"{BASE}/markets/{ids}/quotes/", headers=self._headers())
        if r.status_code != 200:
            log("Smarkets", f"get_quotes failed: {r.status_code}")
            return {}
        return r.json().get("quotes", {})

    async def place_order(self, market_id: str, contract_id: str, side: str,
                          quantity_pence: int, price: int) -> Optional[dict]:
        """
        side: 'buy' (back) or 'sell' (lay)
        quantity_pence: stake in pence (e.g. 1000 = £10)
        price: Smarkets price (1000-9999, where 2000 = 2.0 decimal odds)
        """
        log("Smarkets", f"Placing {side} order on contract {contract_id} "
                        f"@ {price/100:.2f} for £{quantity_pence/100:.2f}")
        r = await self._client.post(f"{BASE}/orders/", headers=self._headers(), json={
            "orders": [{
                "market_id": market_id,
                "contract_id": contract_id,
                "side": side,
                "quantity": quantity_pence,
                "price": price,
            }]
        })
        if r.status_code in (200, 201):
            order = r.json()
            log("Smarkets", f"Order placed: {order}")
            return order
        log("Smarkets", f"Order failed: {r.status_code} {r.text}")
        return None

    async def cancel_order(self, order_id: str) -> bool:
        log("Smarkets", f"Cancelling order {order_id}")
        r = await self._client.delete(f"{BASE}/orders/{order_id}/", headers=self._headers())
        ok = r.status_code in (200, 204)
        log("Smarkets", f"Cancel {'ok' if ok else 'failed'}: {r.status_code}")
        return ok

    async def get_orders(self) -> list:
        log("Smarkets", "Fetching open orders")
        r = await self._client.get(f"{BASE}/orders/", headers=self._headers())
        if r.status_code != 200:
            return []
        return r.json().get("orders", [])

    async def get_account(self) -> dict:
        log("Smarkets", "Fetching account info")
        r = await self._client.get(f"{BASE}/accounts/", headers=self._headers())
        if r.status_code != 200:
            return {}
        return r.json()

    async def close(self):
        await self._client.aclose()
