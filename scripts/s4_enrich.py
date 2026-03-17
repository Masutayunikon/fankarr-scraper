"""
Torrent File Parser
====================
Récupère l'arborescence des fichiers d'un torrent via son infohash,
et extrait les numéros d'épisodes depuis les noms de fichiers .mkv

pip install bencode.py requests
"""

import re
import time
import json
import sys
import hashlib
import requests
import bencodepy as bencode    # pip install bencode.py

from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

Path("data").mkdir(exist_ok=True)

# ─── Config ───────────────────────────────────────────────────────────────────
INPUT_FILE    = "data/torrent_resolved.json"
OUTPUT_FILE   = "data/torrent_enriched.json"
NYA_RAW_PATH  = "data/torrent_raw.json"

# Fichiers .torrent locaux : { raw_name_exact: (chemin_fichier, serie_id, serie_title, type) }
# Ces torrents seront injectés dans le pipeline s'ils sont absents de torrent_resolved.json
LOCAL_TORRENTS: dict[str, tuple[str, int, str, str]] = {
    "Reborn! Kaï (Fan-Kai)": ("reborn_kai.torrent", 60, "Reborn! Kaï", "pack_integrale"),
}

# Torrents manuels avec URL directe (pas de fichier local, pas sur Nyaa Fankai)
# Format : { raw_name: (torrent_url, serie_id, serie_title, season_number, episode_number) }
MANUAL_TORRENTS: dict[str, tuple[str, int, str, int, int]] = {
    "My Hero Academia Henshū - Film 4 - You're Next": (
        "https://nyaa.si/download/1964024.torrent",
        6, "My Hero Academia Henshū", 0, 4
    ),
}

# Sources de téléchargement de .torrent par infohash (fallback dans l'ordre)
TORRENT_SOURCES = [
    "https://itorrents.org/torrent/{HASH}.torrent",
    "https://torrage.info/torrent.php?h={HASH}",
    "https://thetorrent.org/{HASH}.torrent",
]


# ─── Helpers bencode ──────────────────────────────────────────────────────────

def _bget(d: dict, key: str):
    """Récupère une valeur depuis un dict bencode — clé str ou bytes."""
    return d.get(key) or d.get(key.encode()) or d.get(key.encode("utf-8"))

def _bstr(v) -> str:
    """Convertit bytes → str si nécessaire."""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v) if v is not None else ""


# ─── Parsing bencode ──────────────────────────────────────────────────────────

def _parse_bencode_files(data: dict) -> list[dict]:
    """Extrait la liste de fichiers depuis un dict bencode décodé."""
    info = _bget(data, "info") or {}
    files = []

    file_list = _bget(info, "files")
    if file_list:
        for f in file_list:
            path_raw = _bget(f, "path") or []
            path = [_bstr(p) for p in path_raw]
            size = _bget(f, "length") or 0
            files.append({"path": path, "size": size})
    else:
        name = _bget(info, "name")
        if name:
            size = _bget(info, "length") or 0
            files.append({"path": [_bstr(name)], "size": size})

    return files

def _infohash_from_data(data: dict) -> str:
    """Calcule l'infohash SHA1 depuis un dict bencode décodé."""
    info = _bget(data, "info") or {}
    return hashlib.sha1(bencode.encode(info)).hexdigest()


# ─── Récupération + parsing du .torrent ──────────────────────────────────────

def fetch_torrent_files_from_local(filepath: str) -> tuple[list[dict], str]:
    """
    Lit un fichier .torrent local et retourne (fichiers, infohash).
    """
    path = Path(filepath)
    if not path.exists():
        print(f"  [!] Fichier local introuvable : {filepath}")
        return [], ""
    try:
        data = bencode.decode(path.read_bytes())
        infohash = _infohash_from_data(data)
        files = _parse_bencode_files(data)
        print(f"  [✓] Fichier local OK: {filepath} (infohash={infohash})")
        return files, infohash
    except Exception as e:
        print(f"  [!] Erreur lecture fichier local {filepath}: {e}")
        return [], ""


def fetch_torrent_files(infohash: str) -> list[dict]:
    """
    Télécharge le .torrent depuis plusieurs sources et retourne la liste des fichiers.
    """
    h = infohash.upper()

    for source_tpl in TORRENT_SOURCES:
        url = source_tpl.replace("{HASH}", h)
        try:
            r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            if len(r.content) < 100:
                continue
            data = bencode.decode(r.content)
            print(f"  [✓] Source OK: {url}")
            return _parse_bencode_files(data)
        except Exception as e:
            print(f"  [!] Source KO ({url}): {e}")
            continue

    print(f"  [!] Toutes les sources ont échoué pour {infohash}")
    return []


def fetch_torrent_files_from_url(url: str) -> list[dict]:
    """Télécharge un .torrent depuis une URL directe et retourne les fichiers."""
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        data = bencode.decode(r.content)
        return _parse_bencode_files(data)
    except Exception as e:
        print(f"  [!] Erreur URL {url}: {e}")
        return []


# ─── Extraction des épisodes depuis les noms de fichiers ─────────────────────

_EP_PATTERNS = [
    re.compile(r"Henshu\s+(\d{1,3})\b",              re.IGNORECASE),
    re.compile(r"Henshū\s+(\d{1,3})\b",              re.IGNORECASE),
    re.compile(r"Ka[iï]\s+(\d{1,3})\b",              re.IGNORECASE),
    re.compile(r"Yaba[iï]\s+(\d{1,3})\b",            re.IGNORECASE),
    re.compile(r"\bS\d+E(\d+)\b",                    re.IGNORECASE),  # S01E10
    re.compile(r"\b\d{1,2}x(\d{2,3})\b",             re.IGNORECASE),  # 01x10
    re.compile(r"\b(\d{1,3})\s*[-–]\s+\w",           re.IGNORECASE),
    re.compile(r"[-–\s](\d{2,3})[-–\s]",             re.IGNORECASE),
]

# Patterns pour détecter les numéros flottants type 8.5, 2.5...
_EP_FLOAT_PATTERN = re.compile(r"\b(\d{1,3}\.\d)\b")

# Mots-clés bonus/extra — utilise \b pour éviter les faux positifs
_BONUS_KW_RE = re.compile(
    r"\b(?:bonus|extra|special|ova|ona|ncop|nced|opening|ending|film\s+bonus)\b",
    re.IGNORECASE
)

_SEASON_FOLDER_PATTERN = re.compile(r"(?:saison|partie|part)\s*(\d+)", re.IGNORECASE)
_SEASON_FROM_FILE_PATTERN = re.compile(r"\b(\d{1,2})x\d{2,3}\b", re.IGNORECASE)

def extract_season_from_path(path: list[str]) -> int | None:
    for folder in path[:-1]:
        m = _SEASON_FOLDER_PATTERN.search(folder)
        if m:
            return int(m.group(1))
    return None

def _is_bonus_filename(filename: str) -> bool:
    return bool(_BONUS_KW_RE.search(filename))

def extract_episode_number(filename: str) -> int | None:
    if _EP_FLOAT_PATTERN.search(filename):
        return None
    for pat in _EP_PATTERNS:
        m = pat.search(filename)
        if m:
            return int(m.group(1))
    return None

def extract_season_from_filename(filename: str) -> int | None:
    m = _SEASON_FROM_FILE_PATTERN.search(filename)
    return int(m.group(1)) if m else None

_EXCLUDED_FOLDERS = {
    "endings", "ending", "openings", "opening", "ost", "artworks", "artwork",
    "bonus", "extras", "extra", "specials", "special", "ncop", "nced",
    "images", "image", "scans", "scan", "soundtrack", "music",
}

def _is_in_excluded_folder(path: list[str]) -> bool:
    for folder in path[:-1]:
        if folder.lower().strip() in _EXCLUDED_FOLDERS:
            return True
        folder_lower = folder.lower()
        if any(folder_lower.startswith(excl) for excl in _EXCLUDED_FOLDERS):
            return True
    return False

def parse_torrent_structure(files: list[dict]) -> dict:
    episodes  = []
    specials  = []
    folders   = set()
    extras    = []

    for f in files:
        path     = f["path"]
        filename = path[-1] if path else ""
        size     = f["size"]

        for folder in path[:-1]:
            if folder:
                folders.add(folder)

        if filename.lower().endswith((".mkv", ".mp4", ".avi")):
            if _is_in_excluded_folder(path):
                # Exception : fichiers flottants (ex: 7,5 / 19,5) dans dossier bonus → saison 0
                float_m2 = _EP_FLOAT_PATTERN.search(filename)
                if float_m2:
                    specials.append({
                        "num"          : int(float(float_m2.group(1))),
                        "filename"     : filename,
                        "path"         : path,
                        "size"         : size,
                        "season_number": 0,
                    })
                else:
                    extras.append(filename)
                continue

            float_m = _EP_FLOAT_PATTERN.search(filename)
            if _is_bonus_filename(filename) or float_m:
                season_from_path = extract_season_from_path(path)
                sp = {
                    "filename"     : filename,
                    "path"         : path,
                    "size"         : size,
                    "season_number": season_from_path if season_from_path == 0 else 0,
                }
                # Extraire le numéro entier depuis le flottant (ex: 8.5 → 8)
                if float_m:
                    sp["num"] = int(float(float_m.group(1)))
                specials.append(sp)
                continue

            ep_num = extract_episode_number(filename)
            if ep_num is not None:
                season_num = extract_season_from_path(path) or extract_season_from_filename(filename)

                if season_num == 0:
                    specials.append({
                        "num"          : max(ep_num, 1),
                        "filename"     : filename,
                        "path"         : path,
                        "size"         : size,
                        "season_number": 0,
                    })
                else:
                    episodes.append({
                        "num"          : ep_num,
                        "filename"     : filename,
                        "path"         : path,
                        "size"         : size,
                        "season_number": season_num,
                    })
            else:
                extras.append(filename)
        elif filename and not filename.startswith("."):
            extras.append(filename)

    episodes.sort(key=lambda e: e["num"])

    # Numéroter les specials séquentiellement (sauf ceux qui ont déjà un num depuis le flottant)
    seq = 1
    for sp in specials:
        if "num" not in sp:
            sp["num"] = seq
        seq += 1

    all_episodes = episodes + specials

    return {
        "episodes": all_episodes,
        "folders" : sorted(folders),
        "extras"  : extras,
    }


# ─── Enrichissement ──────────────────────────────────────────────────────────

def enrich_with_file_structure(resolved_path: str, nyaa_raw_path: str, output_path: str):
    with open(resolved_path, encoding="utf-8") as f:
        resolved = json.load(f)

    with open(nyaa_raw_path, encoding="utf-8") as f:
        raw_torrents = json.load(f)

    raw_by_title = {}
    for t in raw_torrents:
        title = t.get("name") or t.get("title") or t.get("Name") or ""
        if title:
            raw_by_title[title] = t

    # Injecter les torrents locaux manquants dans le pipeline
    existing_raws = {t.get("raw", "") for t in resolved}
    for raw_name, (local_path, serie_id, serie_title, t_type) in LOCAL_TORRENTS.items():
        if raw_name not in existing_raws:
            print(f"  [LOCAL] Injection de '{raw_name}' dans le pipeline")
            resolved.append({
                "raw"              : raw_name,
                "show_title"       : serie_title,
                "groupe"           : None,
                "type"             : t_type,
                "episodes"         : [],
                "saisons"          : [],
                "torrent_url"      : None,
                "magnet"           : None,
                "infohash"         : None,
                "serie_id"         : serie_id,
                "serie_title"      : serie_title,
                "season_id"        : None,
                "season_number"    : None,
                "resolved_episodes": [],
                "resolved_seasons" : [],
                "resolve_status"   : "ok",
                "torrent_files"    : [],
                "torrent_folders"  : [],
                "torrent_extras"   : [],
                "file_ep_numbers"  : [],
            })

    # Injecter les torrents manuels (URL directe, épisode connu)
    for raw_name, (torrent_url, serie_id, serie_title, season_number, episode_number) in MANUAL_TORRENTS.items():
        if raw_name not in existing_raws:
            print(f"  [MANUAL] Injection de '{raw_name}' dans le pipeline")
            resolved.append({
                "raw"              : raw_name,
                "show_title"       : serie_title,
                "groupe"           : None,
                "type"             : "episode",
                "episodes"         : [episode_number],
                "saisons"          : [],
                "torrent_url"      : torrent_url,
                "magnet"           : None,
                "infohash"         : None,
                "serie_id"         : serie_id,
                "serie_title"      : serie_title,
                "season_id"        : None,
                "season_number"    : season_number,
                "resolved_episodes": [],
                "resolved_seasons" : [],
                "resolve_status"   : None,
                "torrent_files"    : [],
                "torrent_folders"  : [],
                "torrent_extras"   : [],
                "file_ep_numbers"  : [],
            })

    enriched = 0
    for torrent in resolved:
        t_type = torrent.get("type")
        status = torrent.get("resolve_status")

        if t_type not in ("pack_integrale", "pack_saison", "episode", "pack_episodes"):
            continue
        if status not in ("ok", "partial", None):
            continue

        raw_name = torrent.get("raw", "")
        raw      = raw_by_title.get(raw_name)

        # ── Priorité 1 : fichier .torrent local ──────────────────────────────
        local_entry = LOCAL_TORRENTS.get(raw_name)
        local_path  = local_entry[0] if local_entry else None
        if local_path:
            print(f"  → Parsing torrent local: {raw_name[:60]}")
            files, infohash = fetch_torrent_files_from_local(local_path)
            if files:
                # Propager l'infohash si absent
                if infohash and not torrent.get("infohash"):
                    torrent["infohash"] = infohash
                if infohash and not torrent.get("magnet"):
                    import urllib.parse
                    name = torrent.get("raw", "")
                    magnet = f"magnet:?xt=urn:btih:{infohash}&dn={urllib.parse.quote(name)}"
                    # Ajouter les trackers depuis le fichier .torrent si présents
                    try:
                        raw_data = bencode.decode(Path(local_path).read_bytes())
                        announce = _bget(raw_data, "announce")
                        if announce:
                            magnet += f"&tr={urllib.parse.quote(_bstr(announce), safe='')}"
                        announce_list = _bget(raw_data, "announce-list") or []
                        for tier in announce_list:
                            for tracker in (tier if isinstance(tier, list) else [tier]):
                                t_url = _bstr(tracker)
                                if t_url and f"tr={urllib.parse.quote(t_url, safe='')}" not in magnet:
                                    magnet += f"&tr={urllib.parse.quote(t_url, safe='')}"
                    except Exception:
                        pass
                    torrent["magnet"] = magnet
                structure = parse_torrent_structure(files)
                torrent["torrent_files"]   = structure["episodes"]
                torrent["torrent_folders"] = structure["folders"]
                torrent["torrent_extras"]  = structure["extras"]
                torrent["file_ep_numbers"] = [e["num"] for e in structure["episodes"]]
                print(f"     {len(structure['episodes'])} épisodes trouvés: {torrent['file_ep_numbers']}")
                print(f"     Dossiers: {structure['folders']}")
                enriched += 1
                time.sleep(0.1)
                continue

        # ── Priorité 2 : URL directe ou infohash ─────────────────────────────
        torrent_url = (raw.get("torrent_url") or raw.get("torrent")) if raw else None
        torrent_url = torrent_url or torrent.get("torrent_url")
        infohash    = (raw.get("infohash") or raw.get("info_hash") or raw.get("hash")) if raw else None
        infohash    = infohash or torrent.get("infohash")

        if not infohash:
            magnet = (raw.get("magnet", "") if raw else "") or torrent.get("magnet", "")
            m = re.search(r"btih:([a-fA-F0-9]{40})", magnet or "", re.I)
            if m:
                infohash = m.group(1).lower()

        if not torrent_url and not infohash:
            print(f"  [!] Pas de source pour: {raw_name[:60]}")
            continue

        print(f"  → Parsing torrent: {raw_name[:60]}")
        files     = fetch_torrent_files_from_url(torrent_url) if torrent_url else fetch_torrent_files(infohash)
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


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        # Mode test fichier local : python s4_enrich.py --local reborn_kai.torrent
        if arg == "--local" and len(sys.argv) > 2:
            local_file = sys.argv[2]
            files, infohash = fetch_torrent_files_from_local(local_file)
            print(f"  infohash: {infohash}")
        else:
            # Mode test infohash : python s4_enrich.py <infohash>
            files = fetch_torrent_files(arg)

        structure = parse_torrent_structure(files)

        print(f"\n=== DOSSIERS ===")
        for folder in structure["folders"]:
            print(f"  {folder}")

        print(f"\n=== ÉPISODES ({len(structure['episodes'])}) ===")
        for ep in structure["episodes"]:
            print(f"  ep={ep['num']:3d}  {ep['size']//1024//1024:6.0f} MB  {ep['filename'][:70]}")

        print(f"\n=== EXTRAS ===")
        for ex in structure["extras"]:
            print(f"  {ex}")
    else:
        enrich_with_file_structure(INPUT_FILE, NYA_RAW_PATH, OUTPUT_FILE)