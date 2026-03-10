"""
Torrent File Parser
====================
Récupère l'arborescence des fichiers d'un torrent via son infohash,
et extrait les numéros d'épisodes depuis les noms de fichiers .mkv

pip install bencode.py requests
"""

import re
import time
import requests
import bencodepy as bencode    # pip install bencode.py


# ─── Récupération + parsing du .torrent ──────────────────────────────────────
INPUT_FILE  = "data/torrent_resolved.json"
OUTPUT_FILE = "data/torrent_enriched.json"
NYA_RAW_PATH  = "data/torrent_raw.json"

from pathlib import Path
Path("data").mkdir(exist_ok=True)

# Sources de téléchargement de .torrent par infohash (fallback dans l'ordre)
TORRENT_SOURCES = [
    "https://itorrents.org/torrent/{HASH}.torrent",
    "https://torrage.info/torrent.php?h={HASH}",
    "https://thetorrent.org/{HASH}.torrent",
]

def fetch_torrent_files(infohash: str) -> list[dict]:
    """
    Télécharge le .torrent depuis plusieurs sources et retourne la liste des fichiers.
    Chaque fichier : {"path": ["dossier", "sous-dossier", "fichier.mkv"], "size": 123456}
    """
    h = infohash.upper()
    data = None

    for source_tpl in TORRENT_SOURCES:
        url = source_tpl.replace("{HASH}", h)
        try:
            r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            if len(r.content) < 100:
                continue  # réponse vide / erreur HTML
            data = bencode.decode(r.content)
            print(f"  [✓] Source OK: {url}")
            break
        except Exception as e:
            print(f"  [!] Source KO ({url}): {e}")
            continue

    if data is None:
        print(f"  [!] Toutes les sources ont échoué pour {infohash}")
        return []

    return _parse_bencode_files(data)


# ─── Téléchargement depuis URL directe ───────────────────────────────────────

def fetch_torrent_files_from_url(url: str) -> list[dict]:
    """Télécharge un .torrent depuis une URL directe et retourne les fichiers."""
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = bencode.decode(r.content)
    except Exception as e:
        print(f"  [!] Erreur URL {url}: {e}")
        return []
    return _parse_bencode_files(data)

def _parse_bencode_files(data: dict) -> list[dict]:
    """Extrait la liste de fichiers depuis un dict bencode décodé."""
    info  = data.get("info", {})
    files = []
    if "files" in info:
        for f in info["files"]:
            path_raw = f.get("path", [])
            path = [p if isinstance(p, str) else p.decode("utf-8", errors="replace") for p in path_raw]
            size = f.get("length", 0)
            files.append({"path": path, "size": size})
    elif "name" in info:
        name = info["name"] if isinstance(info["name"], str) else info["name"].decode("utf-8", errors="replace")
        size = info.get("length", 0)
        files.append({"path": [name], "size": size})
    return files


# ─── Extraction des épisodes depuis les noms de fichiers ─────────────────────

# Patterns pour trouver le numéro d'épisode dans un nom de fichier .mkv
_EP_PATTERNS = [
    re.compile(r"\b(\d{1,3})\s*[-–]\s+\w",          re.IGNORECASE),  # "01 - Titre"
    re.compile(r"Henshu\s+(\d{1,3})\b",              re.IGNORECASE),  # "Henshu 01"
    re.compile(r"Henshū\s+(\d{1,3})\b",              re.IGNORECASE),  # "Henshū 01"
    re.compile(r"Ka[iï]\s+(\d{1,3})\b",              re.IGNORECASE),  # "Kaï 01"
    re.compile(r"Yaba[iï]\s+(\d{1,3})\b",            re.IGNORECASE),  # "Yabai 01"
    re.compile(r"\bS\d+E(\d+)\b",                    re.IGNORECASE),  # "S01E05"
    re.compile(r"[-–\s](\d{2,3})[-–\s]",             re.IGNORECASE),  # "- 056 -"
]

def extract_episode_number(filename: str) -> int | None:
    """Extrait le numéro d'épisode depuis un nom de fichier."""
    for pat in _EP_PATTERNS:
        m = pat.search(filename)
        if m:
            return int(m.group(1))
    return None

def parse_torrent_structure(files: list[dict]) -> dict:
    """
    Analyse l'arborescence et retourne :
    {
        "episodes": [{"num": 1, "filename": "...", "path": [...], "size": 123}],
        "folders":  ["Saga 1 - ...", "Saga 2 - ..."],
        "extras":   ["Guide.xlsx", "Pack_Plex.zip"]
    }
    """
    episodes = []
    folders  = set()
    extras   = []

    for f in files:
        path     = f["path"]
        filename = path[-1] if path else ""
        size     = f["size"]

        # Dossiers intermédiaires
        for folder in path[:-1]:
            if folder:
                folders.add(folder)

        # Fichiers vidéo → extraire épisode
        if filename.lower().endswith((".mkv", ".mp4", ".avi")):
            ep_num = extract_episode_number(filename)
            if ep_num is not None:
                episodes.append({
                    "num"     : ep_num,
                    "filename": filename,
                    "path"    : path,
                    "size"    : size,
                })
        # Extras (non-vidéo)
        elif filename and not filename.startswith("."):
            extras.append(filename)

    # Trier les épisodes par numéro
    episodes.sort(key=lambda e: e["num"])

    return {
        "episodes": episodes,
        "folders" : sorted(folders),
        "extras"  : extras,
    }


# ─── Intégration avec torrent_resolved.json ──────────────────────────────────

def enrich_with_file_structure(resolved_path: str, nyaa_raw_path: str, output_path: str):
    """
    Pour chaque torrent resolved qui est pack_integrale ou pack_saison sans episodes,
    récupère l'arborescence via l'infohash et enrichit avec les épisodes réels.

    nyaa_raw_path : torrent_raw.json (produit par step1, contient l'infohash)
    """
    import json

    with open(resolved_path, encoding="utf-8") as f:
        resolved = json.load(f)

    with open(nyaa_raw_path, encoding="utf-8") as f:
        raw_torrents = json.load(f)

    # Index raw par titre
    raw_by_title = {}
    for t in raw_torrents:
        title = t.get("name") or t.get("title") or t.get("Name") or ""
        if title:
            raw_by_title[title] = t

    enriched = 0
    for torrent in resolved:
        # On enrichit :
        # - tous les pack_integrale (jamais de season_id ni episode_id)
        # - les pack_saison sans season_id (non résolus)
        # - les torrents "partial" (épisodes manquants en step 3)
        # On skip les pack_saison déjà résolus (Fire Force, Kuroko, Frieren...)
        t_type  = torrent.get("type")
        status  = torrent.get("resolve_status")
        if t_type not in ("pack_integrale", "pack_saison", "episode", "pack_episodes"):
            continue
        if status not in ("ok", "partial", None):
            continue
        # Pack déjà complètement résolu → skip
        if status == "ok" and torrent.get("resolved_episodes"):
            continue
        if status == "ok" and t_type == "pack_saison" and torrent.get("season_id"):
            continue

        raw_name = torrent.get("raw", "")
        raw      = raw_by_title.get(raw_name)
        if not raw:
            continue

        # Priorité : URL torrent Nyaa directe > infohash
        torrent_url = raw.get("torrent_url") or raw.get("torrent")
        infohash    = raw.get("infohash") or raw.get("info_hash") or raw.get("hash")

        # Extraire infohash depuis magnet si pas encore fait
        if not infohash:
            import re as _re
            magnet = raw.get("magnet", "")
            m = _re.search(r"btih:([a-fA-F0-9]{40})", magnet, _re.I)
            if m:
                infohash = m.group(1).lower()

        if not torrent_url and not infohash:
            print(f"  [!] Pas de source pour: {raw_name[:60]}")
            continue

        print(f"  → Parsing torrent: {raw_name[:60]}")
        files = fetch_torrent_files_from_url(torrent_url) if torrent_url else fetch_torrent_files(infohash)
        structure = parse_torrent_structure(files)

        torrent["torrent_files"]   = structure["episodes"]
        torrent["torrent_folders"] = structure["folders"]
        torrent["torrent_extras"]  = structure["extras"]
        torrent["file_ep_numbers"] = [e["num"] for e in structure["episodes"]]

        print(f"     {len(structure['episodes'])} épisodes trouvés: {torrent['file_ep_numbers']}")
        print(f"     Dossiers: {structure['folders']}")
        enriched += 1
        time.sleep(0.5)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(resolved, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {enriched} torrents enrichis → {output_path}")


# ─── Test standalone ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json, sys

    # Mode 1 : tester un infohash directement
    # python torrent_file_parser.py <infohash>
    if len(sys.argv) > 1:
        infohash = sys.argv[1]
        print(f"[Test] infohash: {infohash}")
        files     = fetch_torrent_files(infohash)
        structure = parse_torrent_structure(files)

        print(f"\n=== DOSSIERS ===")
        for folder in structure["folders"]:
            print(f"  {folder}")

        print(f"\n=== ÉPISODES ({len(structure['episodes'])}) ===")
        for ep in structure["episodes"]:
            size_mb = ep["size"] / 1024 / 1024
            print(f"  ep={ep['num']:3d}  {size_mb:6.0f} MB  {ep['filename'][:70]}")

        print(f"\n=== EXTRAS ===")
        for ex in structure["extras"]:
            print(f"  {ex}")

    # Mode 2 : enrichir torrent_resolved.json
    else:
        enrich_with_file_structure(
            resolved_path = INPUT_FILE,
            nyaa_raw_path = NYA_RAW_PATH,
            output_path   = OUTPUT_FILE,
        )