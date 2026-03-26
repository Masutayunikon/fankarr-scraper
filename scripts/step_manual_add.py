"""
Manual Torrent Adder
=====================
Génère une entrée pour data/manual_torrents.json depuis un fichier .torrent local
ou depuis un ID Nyaa.

Usage:
    python step_manual_add.py monfilm.torrent
    python step_manual_add.py --nyaa-id 1234567
    python step_manual_add.py monfilm.torrent --title "Mon Titre" --no-fankai
    python step_manual_add.py --nyaa-id 1234567 --dry-run
"""

import re
import json
import sys
import argparse
import requests
import time
from pathlib import Path

try:
    import torrentool.api as tapi
except ImportError:
    print("[ERR] Installe torrentool : pip install torrentool")
    sys.exit(1)

MANUAL_FILE = "data/manual_torrents.json"
CACHE_DIR   = Path("data/torrent_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "fankarr-manual/1.0"})

VIDEO_EXT = {'.mkv', '.mp4', '.avi', '.m4v', '.mov'}

# ─── Extraction épisodes ──────────────────────────────────────────────────────

def extract_ep_from_filename(fname: str):
    if Path(fname).suffix.lower() not in VIDEO_EXT:
        return None
    stem = Path(fname).stem
    stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()
    if re.search(r'\b\d+[,.]\d+\b', stem): return None
    if re.search(r'\bbonus\b', stem, re.IGNORECASE): return None
    m = re.search(r'\bS\d{1,2}E(\d{2,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b\d{1,2}x(\d{2,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b(?:henshu|henshū|henshû|hensh|kaï|kai|yabai|yabaï|kyodai|film)\s+(\d{1,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\b(\d{1,3})\s*\(fan-?ka[iï]\)', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\(fan-?ka[iï]\)\s+(\d{1,3})\b', stem, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'[-–]\s*0*(\d{1,3})\s*[-–]', stem)
    if m:
        num = int(m.group(1))
        if num <= 999: return num
    return None

def _uses_season_ep_notation(files: list) -> bool:
    return any(
        re.search(r'\b\d{1,2}x\d{2,3}\b', Path(f).stem)
        for f in files if Path(f).suffix.lower() in VIDEO_EXT
    )

def _is_video(f: str) -> bool:
    return (Path(f).suffix.lower() in VIDEO_EXT
            and not re.search(r'\b\d+[,.]\d+\b', Path(f).stem)
            and not re.search(r'\bbonus\b', Path(f).name, re.IGNORECASE))

def extract_ep_numbers(files: list) -> list:
    video_files = sorted([f for f in files if _is_video(f)])
    if _uses_season_ep_notation(files):
        return list(range(1, len(video_files) + 1))
    numbers = set()
    for f in video_files:
        num = extract_ep_from_filename(Path(f).name)
        if num is not None:
            numbers.add(num)
    return sorted(numbers)

# ─── Download depuis Nyaa ─────────────────────────────────────────────────────

def download_from_nyaa(nyaa_id: int) -> Path | None:
    cache_path = CACHE_DIR / f"{nyaa_id}.torrent"
    if cache_path.exists():
        print(f"  [Cache] {nyaa_id}.torrent")
        return cache_path
    url = f"https://nyaa.si/download/{nyaa_id}.torrent"
    print(f"  [Download] {url}")
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        cache_path.write_bytes(r.content)
        time.sleep(0.5)
        return cache_path
    except Exception as e:
        print(f"  [ERR] {e}")
        return None

def fetch_nyaa_info(nyaa_id: int) -> dict:
    """Récupère titre et métadonnées depuis l'API Nyaa."""
    try:
        r = SESSION.get(f"https://nyaa.si/view/{nyaa_id}", timeout=10,
                        headers={"Accept": "application/json"})
        # Nyaa n'a pas d'API JSON publique, on parse juste le titre depuis la page
        import re as _re
        m = _re.search(r'<title>(.*?)</title>', r.text)
        title_raw = m.group(1) if m else None
        if title_raw:
            # Nettoyer ":: Nyaa" en fin
            title_raw = _re.sub(r'\s*::\s*Nyaa\s*$', '', title_raw).strip()
        return {"title": title_raw}
    except Exception:
        return {}

# ─── Parse .torrent ───────────────────────────────────────────────────────────

def parse_torrent(torrent_path: Path):
    torrent = tapi.Torrent.from_file(str(torrent_path))

    try:
        infohash = torrent.info_hash
    except Exception:
        infohash = None

    files = sorted(f.name for f in torrent.files)
    ep_numbers = extract_ep_numbers(files)

    magnet = None
    if infohash:
        from urllib.parse import quote
        name = quote(torrent.name or "")
        magnet = (
            f"magnet:?xt=urn:btih:{infohash}"
            f"&dn={name}"
            f"&tr=http%3A%2F%2Fnyaa.tracker.wf%3A7777%2Fannounce"
            f"&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce"
            f"&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce"
        )

    try:
        total_bytes = sum(f.length for f in torrent.files)
        size = f"{total_bytes / 1024**3:.1f} GiB" if total_bytes >= 1024**3 else f"{total_bytes / 1024**2:.1f} MiB"
    except Exception:
        size = None

    return {
        "name":       torrent.name,
        "infohash":   infohash,
        "magnet":     magnet,
        "size":       size,
        "files":      files,
        "ep_numbers": ep_numbers,
    }

# ─── Manual file ──────────────────────────────────────────────────────────────

def load_manual() -> list:
    p = Path(MANUAL_FILE)
    if not p.exists(): return []
    return [e for e in json.loads(p.read_text(encoding="utf-8")) if isinstance(e, dict)]

def save_manual(entries: list):
    Path(MANUAL_FILE).parent.mkdir(exist_ok=True)
    Path(MANUAL_FILE).write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ajouter un torrent à manual_torrents.json")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("torrent",    nargs="?",      help="Fichier .torrent local")
    group.add_argument("--nyaa-id",  type=int,       help="ID Nyaa (télécharge automatiquement)")
    parser.add_argument("--title",     default=None, help="Titre personnalisé")
    parser.add_argument("--no-fankai", action="store_true", help="Torrent non-officiel Fan-Kai (film, spécial externe...)")
    parser.add_argument("--episode",   type=int,   default=None, help="Forcer le numéro d'épisode (ex: --episode 1)")
    parser.add_argument("--season",    type=int,   default=None, help="Forcer le numéro de saison (ex: --season 0 pour spécial)")
    parser.add_argument("--type",      default=None, choices=["episode","integral","season"], help="Forcer le type (défaut: auto)")
    parser.add_argument("--dry-run",   action="store_true", help="Afficher sans sauvegarder")
    args = parser.parse_args()

    nyaa_id = args.nyaa_id
    nyaa_url = f"https://nyaa.si/view/{nyaa_id}" if nyaa_id else None
    torrent_url = f"https://nyaa.si/download/{nyaa_id}.torrent" if nyaa_id else None

    # Obtenir le fichier .torrent
    if nyaa_id:
        torrent_path = download_from_nyaa(nyaa_id)
        if not torrent_path:
            print("[ERR] Impossible de télécharger le torrent depuis Nyaa")
            sys.exit(1)
        # Récupérer le titre depuis Nyaa si pas fourni
        if not args.title:
            meta = fetch_nyaa_info(nyaa_id)
            args.title = meta.get("title")
    else:
        torrent_path = Path(args.torrent)
        if not torrent_path.exists():
            print(f"[ERR] Fichier introuvable : {torrent_path}")
            sys.exit(1)

    print(f"[Parse] {torrent_path.name}...")
    info = parse_torrent(torrent_path)

    title  = args.title or info["name"] or torrent_path.stem
    fankai = not args.no_fankai

    entry = {
        "title":       title,
        "nyaa_id":     nyaa_id,
        "nyaa_url":    nyaa_url,
        "torrent_url": torrent_url,
        "magnet":      info["magnet"],
        "infohash":    info["infohash"],
        "size":        info["size"],
        "pub_date":    None,
        "seeders":     None,
        "fankai":      fankai,
        "files":       info["files"],
        "ep_numbers":  info["ep_numbers"],
    }
    # Forcer le type/épisode/saison si spécifié
    if args.type:
        entry["force_type"] = args.type
    if args.episode is not None:
        entry["ep_numbers"] = [args.episode]
    if args.season is not None:
        entry["force_season"] = args.season

    print(f"\n{'─'*60}")
    print(f"Titre     : {title}")
    print(f"Nyaa ID   : {nyaa_id or '—'}")
    print(f"Infohash  : {info['infohash']}")
    print(f"Taille    : {info['size']}")
    print(f"Fan-Kai   : {'✅ oui' if fankai else '❌ non (--no-fankai)'}")
    if args.type:     print(f"Type      : {args.type} (forcé)")
    if args.episode is not None: print(f"Épisode   : {args.episode} (forcé)")
    if args.season  is not None: print(f"Saison    : {args.season} (forcé)")
    print(f"Fichiers  : {len(info['files'])}")
    print(f"Épisodes  : {info['ep_numbers'][:15]}{'...' if len(info['ep_numbers']) > 15 else ''}")
    if info["files"]:
        print(f"\nFichiers détectés :")
        for f in info["files"][:10]:
            print(f"  {Path(f).name}")
        if len(info["files"]) > 10:
            print(f"  ... ({len(info['files']) - 10} autres)")
    print(f"{'─'*60}")

    if args.dry_run:
        print("\n[Dry-run] Entrée JSON qui serait ajoutée :")
        print(json.dumps(entry, ensure_ascii=False, indent=2))
        return

    existing = load_manual()
    if info["infohash"] and info["infohash"] in {e.get("infohash") for e in existing}:
        print(f"\n[SKIP] Déjà présent (infohash: {info['infohash'][:16]}...)")
        return
    if title in {e.get("title") for e in existing}:
        print(f"\n[SKIP] Titre déjà présent : {title!r}")
        return

    existing.append(entry)
    save_manual(existing)
    print(f"\n[OK] Ajouté dans {MANUAL_FILE}")
    print(f"     fankai={fankai} | {len(existing)} entrée(s) au total")


if __name__ == "__main__":
    main()