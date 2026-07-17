import re, json, aiohttp, asyncio

HEADERS = {"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"}

RETRYABLE = (400, 403, 408, 429, 500, 502, 503, 504)


async def _fetch(session, url, reader, retries=5):
    """GET with exponential backoff on transient upstream errors."""
    last_err = None
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, timeout=60) as r:
                if r.status in RETRYABLE and attempt < retries - 1:
                    raise aiohttp.ClientResponseError(
                        r.request_info, r.history, status=r.status, message="retryable"
                    )
                r.raise_for_status()
                return await reader(r)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt + 1)
    raise last_err


async def fetch_text(session, url):
    return await _fetch(session, url, lambda r: r.text())

async def fetch_json(session, url):
    return await _fetch(session, url, lambda r: r.json(content_type=None))

async def fetch_upcoming_auctions(session, base):
    html = await fetch_text(session, f"{base}/auctions/upcoming")
    m = re.search(r"viewVars\s*=\s*(\{.*?\});", html, re.S)
    if not m:
        return []
    viewvars = json.loads(m.group(1))
    return viewvars.get("auctions", {}).get("result_page", [])

async def fetch_lots(session, base, auction_id):
    url = f"{base}/ajax/lots/?auctionId={auction_id}&limit=200&page=1"
    data = await fetch_json(session, url)
    return data.get("result_page", [])

async def scrape_site(base):
    async with aiohttp.ClientSession() as session:
        auctions = await fetch_upcoming_auctions(session, base)
        print(f"{base} → {len(auctions)} upcoming auctions")

        all_data = []
        for a in auctions:
            aid = a.get("row_id")
            title = a.get("title")
            
            # Pull the exact date keys we just found
            # Fallback to 'time_start' if it's strictly a timed auction without a live component
            start_date = a.get("time_start_live_auction") or a.get("time_start")
            end_date = a.get("effective_end_time")

            print(f"  ↳ Auction {aid}: {title} (Starts: {start_date})")
            
            lots = await fetch_lots(session, base, aid)
            print(f"     {len(lots)} lots")
            
            for lot in lots:
                all_data.append({
                    "auction_id": aid,
                    "auction_title": title,
                    "start_date": start_date,  # <-- Injects the date
                    "end_date": end_date,      # <-- Injects the end time
                    "lot_id": lot.get("row_id"),
                    "lot_number": lot.get("lot_number"),
                    "title": lot.get("title"),
                    "estimate": lot.get("estimate"),
                    "url": f"{base}/lots/view/{lot.get('row_id')}"
                })
        return all_data

async def main():
    bases = [
        "https://auctions.bassanis.co.za",
        "https://auction.vintageauctions.co.za",
        "https://bid.ewaan.co.za",
    ]
    for base in bases:
        data = await scrape_site(base)
        out = base.split("//")[1].split("/")[0] + "_upcoming.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved {len(data)} lots → {out}")

if __name__ == "__main__":
    asyncio.run(main())
