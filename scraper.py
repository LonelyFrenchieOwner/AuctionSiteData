#!/usr/bin/env python3
# scraper.py
import asyncio, aiohttp, json, os
from datetime import datetime, timezone

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}
CONCURRENCY = 10

def iso_to_date_str(iso_s: str) -> str | None:
    """
    Take an ISO 8601-ish string and return 'YYYY-MM-DD' (UTC) or None.
    Handles trailing 'Z' and timezone offsets.
    """
    if not iso_s or not isinstance(iso_s, str):
        return None
    try:
        # Normalize 'Z' to '+00:00' for fromisoformat
        s = iso_s.strip()
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        # Ensure UTC then take date
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.date().isoformat()  # 'YYYY-MM-DD'
    except Exception:
        # Fall back: try simple split if it's a well-formed 'YYYY-MM-DDTHH:MM:SSZ'
        try:
            return s.split("T", 1)[0]
        except Exception:
            return None

def pick_sold_date(lot: dict) -> str | None:
    """
    Priority for a sold/close timestamp:
      1) lot.extended_end_time
      2) lot.auction.effective_end_time
      3) lot.last_updated
    Returns 'YYYY-MM-DD' (UTC) or None.
    """
    t = (
        lot.get("extended_end_time")
        or (lot.get("auction") or {}).get("effective_end_time")
        or lot.get("last_updated")
    )
    return iso_to_date_str(t) if t else None

async def fetch_page(session: aiohttp.ClientSession, base: str, page: int):
    url = f"{base}/ajax/lots/?lotsType=past&limit=2000&page={page}"
    async with session.get(url, headers=HEADERS, timeout=60) as r:
        r.raise_for_status()
        data = await r.json(content_type=None)
        lots = data.get("result_page", [])
        print(f"{base} → page {page}, got {len(lots)} lots")
        return lots

async def fetch_all_lots(base: str):
    all_lots, page = [], 1
    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [fetch_page(session, base, p) for p in range(page, page + CONCURRENCY)]
            results = await asyncio.gather(*tasks)
            got_any = False
            for lots in results:
                if lots:
                    got_any = True
                    all_lots.extend(lots)
            if not got_any:
                break
            page += CONCURRENCY
    return all_lots

async def scrape_and_merge(base: str, outfile: str):
    # Pull everything then keep sold-only
    new_lots = await fetch_all_lots(base)
    new_lots = [lot for lot in new_lots if lot.get("sold_price")]

    # Load existing (if present) and index by row_id
    existing = []
    if os.path.exists(outfile):
        with open(outfile, "r", encoding="utf-8") as f:
            existing = json.load(f)

    existing_by_id = {row.get("row_id"): row for row in existing}
    added = 0
    updated = 0

    for lot in new_lots:
        rid = lot.get("row_id")
        if not rid:
            continue

        sold_date = pick_sold_date(lot)

        payload = {
            "row_id": rid,
            "title": lot.get("title"),
            "status": lot.get("status"),
            "sold_price": lot.get("sold_price"),
            "url": f"{base}/lots/view/{rid}",
            "sold_date": sold_date,  # <-- just the date
            # handy context; optional
            "auction_title": (lot.get("auction") or {}).get("title"),
        }

        if rid in existing_by_id:
            row = existing_by_id[rid]
            changed = False
            # Upsert fields; only overwrite sold_date if missing/empty
            for k, v in payload.items():
                if k not in row or (k == "sold_date" and (row.get("sold_date") in (None, "", "null")) and v):
                    row[k] = v
                    changed = True
            if changed:
                updated += 1
        else:
            existing.append(payload)
            existing_by_id[rid] = payload
            added += 1

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    print(f"✅ {added} new lots added, {updated} updated ({len(existing)} total) → {outfile}")

async def main():
    bases = [
        "https://auctions.bassanis.co.za",
        "https://auction.vintageauctions.co.za",
        "https://bid.ewaan.co.za",
    ]
    for base in bases:
        domain = base.split("//")[1].split("/")[0]
        outfile = f"{domain}_lots.json"
        await scrape_and_merge(base, outfile)

if __name__ == "__main__":
    asyncio.run(main())
