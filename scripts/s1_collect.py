"""
ÉTAPE 1 - Fankai Torrent Collector
===================================
Récupère les liens Nyaa depuis un channel Discord,
interroge l'API Nyaa non-officielle, et dump tous les noms
de torrents dans un fichier texte pour analyse regex.
"""

import re
import json
import time
import requests
import os
from urllib.parse import unquote_plus
from dotenv import load_dotenv
load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
CHANNEL_ID = os.environ["CHANNEL_ID"]   # ID du channel
NYAA_API_BASE = "https://nyaaapi.onrender.com"
OUTPUT_FILE = "data/torrent_names.txt"
RAW_JSON_FILE = "data/torrent_raw.json"

from pathlib import Path
Path("data").mkdir(exist_ok=True)
# ──────────────────────────────────────────────────────────────────────────────


# ── 1. Récupération des messages Discord ──────────────────────────────────────

def fetch_discord_messages(token: str, channel_id: str, limit_per_req: int = 100) -> list[dict]:
    """Récupère TOUS les messages d'un channel (pagination automatique)."""
    headers = {"Authorization": token}
    url     = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    messages = []
    last_id  = None

    print("[Discord] Récupération des messages...")
    while True:
        params = {"limit": limit_per_req}
        if last_id:
            params["before"] = last_id

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"[Discord] Erreur {resp.status_code}: {resp.text}")
            break

        batch = resp.json()
        if not batch:
            break

        messages.extend(batch)
        last_id = batch[-1]["id"]
        print(f"  → {len(messages)} messages récupérés...", end="\r")

        if len(batch) < limit_per_req:
            break
        time.sleep(0.5)   # rate-limit friendly

    print(f"\n[Discord] Total : {len(messages)} messages")
    return messages


# ── 2. Extraction des liens Nyaa ──────────────────────────────────────────────

# Deux patterns :
#   A) /view/{id}         → torrent direct
#   B) /user/Fan-Kai?q=   → recherche user
NYAA_VIEW_RE   = re.compile(r"https://nyaa\.si/view/(\d+)")
NYAA_SEARCH_RE = re.compile(r"https://nyaa\.si/user/([^?\s>]+)\?([^\s>\")\]]+)")

# Overrides manuels : quand le q= extrait est trop générique ou ambigu,
# forcer un q= plus précis.
# Format : {q_original: q_remplacé}
SEARCH_Q_OVERRIDES: dict[str, str] = {
    "shippuden": "shippuden yab",
}

# Overrides manuels : remplacer une recherche par un torrent direct (view/{id})
# Utile quand la recherche retourne trop de résultats parasites
# Format : {q_original: nyaa_view_id}
SEARCH_TO_VIEW_OVERRIDES: dict[str, str] = {
    "Tokyo Revengers ": "2083491",
}

def extract_nyaa_links(messages: list[dict]) -> tuple[list, list]:
    """Retourne (view_ids, search_queries) extraits de tous les messages."""
    view_ids = []
    searches = []   # liste de dicts {user, params_raw, q}

    for msg in messages:
        content = msg.get("content", "")

        for m in NYAA_VIEW_RE.finditer(content):
            torrent_id = m.group(1)
            if torrent_id not in view_ids:
                view_ids.append(torrent_id)

        for m in NYAA_SEARCH_RE.finditer(content):
            user       = m.group(1)
            params_raw = m.group(2)
            # extraire q=
            q_match = re.search(r"[?&]q=([^&]+)", "?" + params_raw)
            q = unquote_plus(q_match.group(1)) if q_match else ""
            # Appliquer les overrides manuels si nécessaire
            q = SEARCH_Q_OVERRIDES.get(q, q)
            # Remplacer la recherche par un torrent direct si override défini
            if q in SEARCH_TO_VIEW_OVERRIDES:
                view_id = SEARCH_TO_VIEW_OVERRIDES[q]
                if view_id not in view_ids:
                    view_ids.append(view_id)
                    print(f"  [override] recherche '{q}' → torrent direct {view_id}")
                continue  # ne pas ajouter la recherche
            entry = {"user": user, "params_raw": params_raw, "q": q}
            if entry not in searches:
                searches.append(entry)

    print(f"[Extraction] {len(view_ids)} torrents directs / {len(searches)} recherches user")
    return view_ids, searches


# ── 3. Appels API Nyaa ────────────────────────────────────────────────────────

def fetch_torrent_by_id(torrent_id: str) -> dict | None:
    url = f"{NYAA_API_BASE}/nyaa/id/{torrent_id}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        # L'API retourne parfois { "data": { ... } }
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            return data["data"]
        return data
    except Exception as e:
        print(f"  [!] Erreur ID {torrent_id}: {e}")
        return None

def fetch_user_search(user: str, q: str, extra_params: str = "") -> list[dict]:
    """Récupère tous les torrents d'une recherche user (pagination via page)."""
    results = []
    page    = 1
    while True:
        params = {"q": q, "p": page}
        url    = f"{NYAA_API_BASE}/nyaa/user/{user}"
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  [!] Erreur recherche '{q}': {e}")
            break

        # L'API retourne soit une liste directement, soit {"torrents": [...]}
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("torrents", data.get("data", []))
        else:
            break

        if not batch:
            break

        results.extend(batch)
        if len(batch) < 75:   # moins d'une page pleine → fin
            break
        page += 1
        time.sleep(0.3)

    return results


def collect_all_torrents(view_ids: list, searches: list) -> list[dict]:
    all_torrents = []

    # Torrents directs
    print(f"\n[Nyaa] Récupération des {len(view_ids)} torrents directs...")
    for i, tid in enumerate(view_ids, 1):
        print(f"  [{i}/{len(view_ids)}] ID {tid}")
        data = fetch_torrent_by_id(tid)
        if data:
            # normalise : l'API retourne parfois l'objet directement
            if isinstance(data, list):
                all_torrents.extend(data)
            else:
                all_torrents.append(data)
        time.sleep(0.3)

    # Recherches user
    print(f"\n[Nyaa] Récupération des {len(searches)} recherches user...")
    for i, s in enumerate(searches, 1):
        print(f"  [{i}/{len(searches)}] user={s['user']} q='{s['q']}'")
        results = fetch_user_search(s["user"], s["q"])
        print(f"    → {len(results)} résultats")
        all_torrents.extend(results)
        time.sleep(0.5)

    # Déduplication par nom
    seen  = set()
    dedup = []
    for t in all_torrents:
        name = t.get("name") or t.get("title") or t.get("Name") or ""
        if name and name not in seen:
            seen.add(name)
            dedup.append(t)

    print(f"\n[Nyaa] Total après dédup : {len(dedup)} torrents")
    return dedup


# ── 4. Dump des résultats ─────────────────────────────────────────────────────

def dump_results(torrents: list[dict]):
    # Fichier texte : un nom par ligne (pour analyse regex)
    names = []
    for t in torrents:
        name = t.get("name") or t.get("title") or t.get("Name") or ""
        if name:
            names.append(name)

    import re as _re
    _MAGNET_RE = _re.compile(r"btih:([a-fA-F0-9]{40})", _re.IGNORECASE)

    # Enrichir chaque torrent avec l'infohash extrait du magnet
    for t in torrents:
        if not t.get("infohash"):
            magnet = t.get("magnet", "")
            m = _MAGNET_RE.search(magnet)
            if m:
                t["infohash"] = m.group(1).lower()
        # Ajouter aussi le lien torrent direct Nyaa si dispo
        if not t.get("torrent_url"):
            link = t.get("link", "") or t.get("torrent", "")
            if "nyaa.si/download/" in link:
                t["torrent_url"] = link
            elif "nyaa.si/view/" in link:
                nyaa_id = link.rstrip("/").split("/")[-1]
                t["torrent_url"] = f"https://nyaa.si/download/{nyaa_id}.torrent"

    names.sort()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"# {len(names)} noms de torrents Fankai/Nyaa\n\n")
        for n in names:
            f.write(n + "\n")

    print(f"[Output] {len(names)} noms → {OUTPUT_FILE}")

    # Fichier JSON complet pour debug
    with open(RAW_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(torrents, f, ensure_ascii=False, indent=2)

    print(f"[Output] JSON brut → {RAW_JSON_FILE}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    # 1. Discord
    messages = fetch_discord_messages(DISCORD_TOKEN, CHANNEL_ID)

    # 2. Extraction liens
    view_ids, searches = extract_nyaa_links(messages)

    if not view_ids and not searches:
        print("[!] Aucun lien Nyaa trouvé dans le channel. Vérifie le token/channel ID.")
        return

    # 3. Appels Nyaa API
    torrents = collect_all_torrents(view_ids, searches)

    # 4. Dump
    dump_results(torrents)

    print("\n✅ Étape 1 terminée ! Ouvre torrent_names.txt pour analyser les cas.")


if __name__ == "__main__":
    main()