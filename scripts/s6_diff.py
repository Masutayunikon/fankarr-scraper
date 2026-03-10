"""
ÉTAPE 6 - Fankai Diff Generator
=================================
Compare l'ancien et le nouveau torrent_final.json,
génère un résumé markdown pour la Pull Request GitHub.

Input  : torrent_final.json (nouveau, généré par s5)
         torrent_final.old.json (ancien, récupéré depuis git)
Output : pr_body.md  — corps de la Pull Request
         exit code 0 si des changements détectés, 1 si rien à faire

Usage :
  python s6_diff.py --old torrent_final.old.json --new torrent_final.json
"""

import re
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
from collections import defaultdict


# ─── Chargement ───────────────────────────────────────────────────────────────

from pathlib import Path
Path("data").mkdir(exist_ok=True)

def load(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


# ─── Index par série ──────────────────────────────────────────────────────────

def index_by_serie(torrents: list[dict]) -> dict[int, list[dict]]:
    """{ serie_id: [torrents...] }"""
    result: dict[int, list[dict]] = defaultdict(list)
    for t in torrents:
        sid = t.get("serie_id")
        if sid is not None:
            result[sid].append(t)
    return result


def index_episode_ids(torrents: list[dict]) -> set[int]:
    """Tous les episode_id résolus dans une liste de torrents."""
    ids = set()
    for t in torrents:
        for ep in t.get("resolved_episodes") or []:
            eid = ep.get("episode_id")
            if eid is not None:
                ids.add(eid)
    return ids


def serie_title(torrents: list[dict]) -> str:
    for t in torrents:
        title = t.get("serie_title") or t.get("show_title")
        if title:
            return title
    return f"Série #{torrents[0].get('serie_id', '?')}"


# ─── Diff ─────────────────────────────────────────────────────────────────────

def compute_diff(old: list[dict], new: list[dict]) -> dict:
    """
    Retourne un dict avec :
      - new_series      : [serie_id, ...] séries absentes de l'ancien
      - removed_series  : [serie_id, ...] séries absentes du nouveau
      - updated_series  : { serie_id: { added_eps, removed_eps, new_torrents } }
      - unchanged_count : int
    """
    old_idx = index_by_serie(old)
    new_idx = index_by_serie(new)

    old_sids = set(old_idx.keys())
    new_sids = set(new_idx.keys())

    new_series     = sorted(new_sids - old_sids)
    removed_series = sorted(old_sids - new_sids)
    common_sids    = old_sids & new_sids

    updated_series = {}
    unchanged_count = 0

    for sid in sorted(common_sids):
        old_eps = index_episode_ids(old_idx[sid])
        new_eps = index_episode_ids(new_idx[sid])

        added_eps   = new_eps - old_eps
        removed_eps = old_eps - new_eps

        # Torrents ajoutés (par infohash)
        old_hashes = {t.get("infohash") for t in old_idx[sid] if t.get("infohash")}
        new_hashes = {t.get("infohash") for t in new_idx[sid] if t.get("infohash")}
        new_torrents = new_hashes - old_hashes

        if added_eps or removed_eps or new_torrents:
            updated_series[sid] = {
                "added_eps"   : sorted(added_eps),
                "removed_eps" : sorted(removed_eps),
                "new_torrents": len(new_torrents),
                "title"       : serie_title(new_idx[sid]),
            }
        else:
            unchanged_count += 1

    return {
        "new_series"     : new_series,
        "removed_series" : removed_series,
        "updated_series" : updated_series,
        "unchanged_count": unchanged_count,
        "new_idx"        : new_idx,
        "old_idx"        : old_idx,
    }


# ─── Formatage épisodes ───────────────────────────────────────────────────────

def format_ep_list(ep_ids: list[int], limit: int = 10) -> str:
    """Formate une liste d'episode_id de façon lisible."""
    if not ep_ids:
        return ""
    if len(ep_ids) <= limit:
        return ", ".join(f"`{e}`" for e in ep_ids)
    return ", ".join(f"`{e}`" for e in ep_ids[:limit]) + f" … (+{len(ep_ids) - limit})"


# ─── Génération du corps de PR ────────────────────────────────────────────────

def generate_pr_body(diff: dict) -> str:
    now   = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = []

    # En-tête
    lines.append(f"## 🎬 Mise à jour torrents — {now}")
    lines.append("")

    new_idx = diff["new_idx"]
    old_idx = diff["old_idx"]

    # ── Nouvelles séries
    if diff["new_series"]:
        lines.append("### 🆕 Nouvelles séries")
        for sid in diff["new_series"]:
            torrents = new_idx.get(sid, [])
            title    = serie_title(torrents)
            n_eps    = len(index_episode_ids(torrents))
            n_tor    = len(torrents)
            lines.append(f"- **{title}** — {n_eps} épisodes, {n_tor} torrent(s)")
        lines.append("")

    # ── Séries mises à jour
    if diff["updated_series"]:
        lines.append("### ✅ Séries mises à jour")
        for sid, info in diff["updated_series"].items():
            lines.append(f"- **{info['title']}**")
            if info["added_eps"]:
                lines.append(f"  - ➕ {len(info['added_eps'])} épisode(s) ajouté(s) : {format_ep_list(info['added_eps'])}")
            if info["removed_eps"]:
                lines.append(f"  - ➖ {len(info['removed_eps'])} épisode(s) retiré(s) : {format_ep_list(info['removed_eps'])}")
            if info["new_torrents"]:
                lines.append(f"  - 📦 {info['new_torrents']} nouveau(x) torrent(s)")
        lines.append("")

    # ── Séries supprimées
    if diff["removed_series"]:
        lines.append("### ❌ Séries supprimées")
        for sid in diff["removed_series"]:
            torrents = old_idx.get(sid, [])
            title    = serie_title(torrents)
            lines.append(f"- **{title}**")
        lines.append("")

    # ── Résumé
    lines.append("### 📊 Résumé")
    total_new_eps = sum(
        len(info["added_eps"]) for info in diff["updated_series"].values()
    ) + sum(
        len(index_episode_ids(new_idx.get(sid, [])))
        for sid in diff["new_series"]
    )
    lines.append(f"| Nouvelles séries | Séries MAJ | Épisodes ajoutés | Séries inchangées |")
    lines.append(f"|:---:|:---:|:---:|:---:|")
    lines.append(
        f"| {len(diff['new_series'])} "
        f"| {len(diff['updated_series'])} "
        f"| {total_new_eps} "
        f"| {diff['unchanged_count']} |"
    )
    lines.append("")
    lines.append("---")
    lines.append("*Généré automatiquement par `s6_diff.py`*")

    return "\n".join(lines)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Génère le diff pour la PR GitHub")
    parser.add_argument("--old", default="torrent_final.old.json",
                        help="Ancien torrent_final.json (depuis git)")
    parser.add_argument("--new", default="torrent_final.json",
                        help="Nouveau torrent_final.json (généré par s5)")
    parser.add_argument("--output", default="pr_body.md",
                        help="Fichier de sortie markdown")
    parser.add_argument("--quiet", action="store_true",
                        help="Pas de print, juste exit code")
    args = parser.parse_args()

    old = load(args.old)
    new = load(args.new)

    if not old and not new:
        print("[s6] Aucun fichier trouvé, rien à faire.")
        sys.exit(1)

    diff = compute_diff(old, new)

    has_changes = (
        bool(diff["new_series"]) or
        bool(diff["removed_series"]) or
        bool(diff["updated_series"])
    )

    if not has_changes:
        if not args.quiet:
            print("[s6] Aucun changement détecté.")
        sys.exit(1)  # exit 1 = pas de PR à ouvrir

    body = generate_pr_body(diff)
    Path(args.output).write_text(body, encoding="utf-8")

    if not args.quiet:
        print(f"[s6] Changements détectés → {args.output}")
        print(f"     Nouvelles séries   : {len(diff['new_series'])}")
        print(f"     Séries mises à jour: {len(diff['updated_series'])}")
        print(f"     Séries supprimées  : {len(diff['removed_series'])}")

    sys.exit(0)  # exit 0 = ouvrir la PR


if __name__ == "__main__":
    main()