"""
ÉTAPE 1 - Fankai Torrent Collector
===================================
Récupère tous les torrents uploadés par Fan-Kai sur Nyaa
via l'API non-officielle nyaaapi.onrender.com avec pagination.

Output : data/torrent_raw.json
         data/torrent_names.txt
"""

import re
import json
import time
import requests
from pathlib import Path

Path("data").mkdir(exist_ok=True)

# ─── Config ───────────────────────────────────────────────────────────────────

API_BASE       = "https://nyaaapi.onrender.com"
NYAA_USER      = "Fan-Kai"
OUTPUT_FILE    = "data/torrent_raw.json"
NAMES_FILE     = "data/torrent_names.txt"
DELAY          = 0.5

# ─── Helpers ──────────────────────────────────────────────────────────────────

_MAGNET_RE = re.compile(r"btih:([a-fA-F0-9]{40})", re.IGNORECASE)

def extract_infohash(magnet: str) -> str | None:
    m = _MAGNET_RE.search(magnet or "")
    return m.group(1).lower() if m else None

def normalize(item: dict) -> dict:
    magnet   = item.get("magnet") or ""
    infohash = extract_infohash(magnet)
    link     = item.get("link") or item.get("torrent") or ""

    # Extraire l'ID Nyaa depuis le lien
    nyaa_id = None
    m = re.search(r"/view/(\d+)", link)
    if m:
        nyaa_id = int(m.group(1))

    # URL torrent direct
    torrent_url = item.get("torrent") or item.get("torrent_url")
    if not torrent_url and nyaa_id:
        torrent_url = f"https://nyaa.si/download/{nyaa_id}.torrent"

    title = (item.get("title") or item.get("name") or item.get("Name") or "").strip()

    return {
        "title"      : title,
        "nyaa_url"   : f"https://nyaa.si/view/{nyaa_id}" if nyaa_id else None,
        "torrent_url": torrent_url,
        "magnet"     : magnet or None,
        "infohash"   : infohash,
        "nyaa_id"    : nyaa_id,
        "pub_date"   : item.get("time"),
        "seeders"    : item.get("seeders"),
        "size"       : item.get("size"),
    }

def fetch_user_page(user: str, page: int) -> list[dict]:
    url = f"{API_BASE}/nyaa/user/{user}"
    try:
        r = requests.get(
            url,
            params={"page": page, "sort": "id", "order": "desc"},
            timeout=20,
            headers={"User-Agent": "fankarr-scraper/1.0"}
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        return data.get("data", data.get("torrents", []))
    except Exception as e:
        print(f"  [!] Erreur page {page}: {e}")
        return []

def load_existing(path: str) -> list[dict]:
    try:
        p = Path(path)
        if not p.exists():
            return []
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

def merge_torrents(existing: list[dict], new: list[dict]) -> tuple[list[dict], int]:
    by_hash  = {t["infohash"]: True for t in existing if t.get("infohash")}
    by_title = {t["title"]: True    for t in existing if t.get("title")}

    added = 0
    for t in new:
        if t.get("infohash") and t["infohash"] in by_hash:
            continue
        if t.get("title") and t["title"] in by_title:
            continue
        existing.append(t)
        if t.get("infohash"): by_hash[t["infohash"]]  = True
        if t.get("title"):    by_title[t["title"]]     = True
        added += 1

    return existing, added

def dump_names(torrents: list[dict]):
    names = sorted(t["title"] for t in torrents if t.get("title"))
    with open(NAMES_FILE, "w", encoding="utf-8") as f:
        f.write(f"# {len(names)} noms de torrents Fankai/Nyaa\n\n")
        for n in names:
            f.write(n + "\n")
    print(f"[Output] {len(names)} noms → {NAMES_FILE}")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    existing = load_existing(OUTPUT_FILE)
    print(f"[Existing] {len(existing)} torrents déjà en base")

    existing_hashes = {t["infohash"] for t in existing if t.get("infohash")}
    existing_titles = {t["title"]    for t in existing if t.get("title")}

    all_new = []
    page    = 1

    while True:
        print(f"[API] Page {page} (user={NYAA_USER})...")
        items = fetch_user_page(NYAA_USER, page)

        if not items:
            print(f"[API] Page {page} vide → arrêt")
            break

        page_new = []
        stop = False
        for item in items:
            t = normalize(item)
            if not t["title"]:
                continue
            if (t.get("infohash") and t["infohash"] in existing_hashes) or \
               (t.get("title")    and t["title"]    in existing_titles):
                stop = True
                break
            page_new.append(t)

        all_new.extend(page_new)
        print(f"       {len(page_new)} nouveaux sur cette page")

        if stop:
            print("[API] Torrent déjà connu → arrêt pagination")
            break

        if len(items) < 75:
            print(f"[API] Dernière page ({len(items)} < 75) → arrêt")
            break

        page += 1
        time.sleep(DELAY)

    merged, added = merge_torrents(existing, all_new)
    merged.sort(key=lambda t: t.get("nyaa_id") or 0, reverse=True)

    Path(OUTPUT_FILE).write_text(
        json.dumps(merged, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\n[OK] {added} nouveaux torrents ajoutés → {OUTPUT_FILE}")
    print(f"[OK] Total : {len(merged)} torrents")

    dump_names(merged)
    print("\n✅ Étape 1 terminée !")

if __name__ == "__main__":
    main()