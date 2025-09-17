#!/usr/bin/env python3
# scraper.py
import asyncio, aiohttp, sys, json, os
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}
CONCURRENCY = 10

async def fetch_page(session, base, page):
    url = f"{base}/ajax/lots/?lotsType=past&limit=2000&page={page}"
    async with session.get(url, headers=HEADERS, timeout=60) as r:
        r.raise_for_status()
        data = await r.json(content_type=None)
        lots = data.get("result_page", [])
        print(f"{base} → page {page}, got {len(lots)} lots")
        return lots

async def fetch_all_lots(base):
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

async def scrape_and_merge(base, outfile):
    new_lots = await fetch_all_lots(base)
    new_lots = [lot for lot in new_lots if lot.get("sold_price")]

    # Load existing JSON if present
    existing = []
    if os.path.exists(outfile):
        with open(outfile, "r", encoding="utf-8") as f:
            existing = json.load(f)

    seen = {row["row_id"] for row in existing}
    added = []

    for lot in new_lots:
        if lot["row_id"] not in seen:
            entry = {
                "row_id": lot.get("row_id"),
                "title": lot.get("title"),
                "status": lot.get("status"),
                "sold_price": lot.get("sold_price"),
                "url": f"{base}/lots/view/{lot['row_id']}",
                "scraped_at": datetime.utcnow().isoformat()
            }
            existing.append(entry)
            added.append(entry)

    # Save back
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    print(f"✅ {len(added)} new lots added ({len(existing)} total)")

async def main():
    bases = [
        "https://auctions.bassanis.co.za",
        "https://auction.vintageauctions.co.za",
        "https://bid.ewaan.co.za"
    ]
    for base in bases:
        domain = base.split("//")[1].split("/")[0]
        outfile = f"{domain}_lots.json"
        await scrape_and_merge(base, outfile)

if __name__ == "__main__":
    asyncio.run(main())
