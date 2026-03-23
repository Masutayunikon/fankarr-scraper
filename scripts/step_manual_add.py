"""
Manual Torrent Adder
=====================
Génère une entrée pour data/manual_torrents.json depuis un fichier .torrent local.
Parse automatiquement les fichiers internes et extrait les numéros d'épisodes.

Usage:
    python step_manual_add.py monfilm.torrent
    python step_manual_add.py monfilm.torrent --serie "GTO Kai" --type integral
    python step_manual_add.py monfilm.torrent --dry-run   # affiche sans sauvegarder
"""

import re
import json
import sys
import argparse
import hashlib
from pathlib import Path

try:
    import torrentool.api as tapi
except ImportError:
    print("[ERR] Installe torrentool : pip install torrentool")
    sys.exit(1)

MANUAL_FILE = "data/manual_torrents.json"

# ─── Même logique que step1b ──────────────────────────────────────────────────

VIDEO_EXT = {'.mkv', '.mp4', '.avi', '.m4v', '.mov'}

def extract_ep_from_filename(fname: str):
    if Path(fname).suffix.lower() not in VIDEO_EXT:
        return None
    stem = Path(fname).stem
    stem = re.sub(r'^\[[^\]]*\]\s*', '', stem).strip()
    if re.search(r'\b\d+,\d+\b', stem):
        return None
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


def parse_torrent(torrent_path: Path):
    torrent = tapi.Torrent.from_file(str(torrent_path))

    # Infohash depuis le fichier .torrent
    import hashlib, struct
    try:
        raw = torrent_path.read_bytes()
        # Extraire le dictionnaire 'info' du bencoding pour calculer l'infohash
        info_start = raw.find(b'4:info') + 6
        # Utiliser torrentool si possible
        infohash = torrent.info_hash
    except Exception:
        infohash = None

    files = sorted(f.name for f in torrent.files)
    ep_numbers = sorted({
        n for f in files
        if (n := extract_ep_from_filename(Path(f).name)) is not None
    })

    # Générer magnet depuis infohash
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

    # Taille totale
    try:
        total_bytes = sum(f.length for f in torrent.files)
        if total_bytes >= 1024**3:
            size = f"{total_bytes / 1024**3:.1f} GiB"
        else:
            size = f"{total_bytes / 1024**2:.1f} MiB"
    except Exception:
        size = None

    return {
        "name":      torrent.name,
        "infohash":  infohash,
        "magnet":    magnet,
        "size":      size,
        "files":     files,
        "ep_numbers": ep_numbers,
    }


def load_manual() -> list:
    p = Path(MANUAL_FILE)
    if not p.exists():
        return []
    data = json.loads(p.read_text(encoding="utf-8"))
    return [e for e in data if isinstance(e, dict)]


def save_manual(entries: list):
    Path(MANUAL_FILE).parent.mkdir(exist_ok=True)
    Path(MANUAL_FILE).write_text(
        json.dumps(entries, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ajouter un torrent local à manual_torrents.json")
    parser.add_argument("torrent", help="Fichier .torrent local")
    parser.add_argument("--title",   default=None, help="Titre personnalisé (défaut: nom du torrent)")
    parser.add_argument("--dry-run", action="store_true", help="Afficher sans sauvegarder")
    args = parser.parse_args()

    torrent_path = Path(args.torrent)
    if not torrent_path.exists():
        print(f"[ERR] Fichier introuvable : {torrent_path}")
        sys.exit(1)

    print(f"[Parse] {torrent_path.name}...")
    info = parse_torrent(torrent_path)

    title = args.title or info["name"] or torrent_path.stem

    entry = {
        "title":       title,
        "nyaa_id":     None,
        "nyaa_url":    None,
        "torrent_url": None,
        "magnet":      info["magnet"],
        "infohash":    info["infohash"],
        "size":        info["size"],
        "pub_date":    None,
        "seeders":     None,
        "files":       info["files"],
        "ep_numbers":  info["ep_numbers"],
    }

    print(f"\n{'─'*60}")
    print(f"Titre     : {title}")
    print(f"Infohash  : {info['infohash']}")
    print(f"Taille    : {info['size']}")
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

    # Vérifier doublon
    existing = load_manual()
    existing_hashes = {e.get("infohash") for e in existing if e.get("infohash")}
    existing_titles = {e.get("title") for e in existing if e.get("title")}

    if info["infohash"] and info["infohash"] in existing_hashes:
        print(f"\n[SKIP] Déjà présent (infohash: {info['infohash'][:16]}...)")
        return
    if title in existing_titles:
        print(f"\n[SKIP] Titre déjà présent : {title!r}")
        return

    existing.append(entry)
    save_manual(existing)
    print(f"\n[OK] Ajouté dans {MANUAL_FILE}")
    print(f"[OK] Total entrées : {len([e for e in existing if not any(k.startswith('_') for k in e)])}")


if __name__ == "__main__":
    main()