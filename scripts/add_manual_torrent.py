"""
Script d'ajout batch de torrents manuels
==========================================
Lance step_manual_add.py pour chaque torrent à ajouter.
Modifier la liste TORRENTS ci-dessous pour ajouter vos entrées.
"""

import subprocess
import sys

# ─── À REMPLIR ────────────────────────────────────────────────────────────────
# Chaque entrée est un dict avec les paramètres à passer à step_manual_add.py
# Paramètres disponibles :
#   source     : chemin .torrent local OU nyaa_id (int)
#   title      : titre de la série (doit matcher le titre sur fankai)
#   no_fankai  : True/False
#   episode    : numéro d'épisode forcé (optionnel)
#   season     : numéro de saison forcé (optionnel)
#   type       : "episode", "integral", "season" (optionnel)

TORRENTS = [
    {
        "source":    "GTO_Kai_upscale.torrent",
        "title":     "GTO Kaï",
        "no_fankai": False,
        # "episode": 1,
        # "season":  1,
        # "type":    "integral",
    },
    {
        "source":    "reborn_kai.torrent",
        "title":     "Reborn! Kaï",
        "no_fankai": False,
        # "episode": 1,
        # "season":  1,
        # "type":    "integral",
    },
    {
        "source": 1964024,
        "title": "My Hero Academia Henshū",
        "no_fankai": True,
        "season": 0,
        "episode": 4,
    },
    {
        "source": 2068604,
        "title": "One Piece Kaï Strong World",
        "no_fankai": True,
        "season": 0,
        "episode": 1,
    },
    {
        "source": 1250812,
        "title": "One Piece Kaï Z",
        "no_fankai": True,
        "season": 0,
        "episode": 2,
    },
    {
        "source": 1815826,
        "title": "One Piece Kaï Gold",
        "no_fankai": True,
        "season": 0,
        "episode": 3,
    },
    {
        "source": 1257276,
        "title": "One Piece Kaï Stampede",
        "no_fankai": True,
        "season": 0,
        "episode": 4,
    },
    {
        "source": 1648250,
        "title": "One Piece Kaï Red",
        "no_fankai": True,
        "season": 0,
        "episode": 5,
    }
    # ── Exemples pour films/spéciaux non-fankai ───────────────────────────────
    # {
    #     "source":    12345678,           # nyaa_id
    #     "title":     "One Piece Kaï",
    #     "no_fankai": True,
    #     "season":    0,
    #     "episode":   1,
    # },
    # {
    #     "source":    "film.torrent",
    #     "title":     "Boruto Kaï",
    #     "no_fankai": True,
    #     "season":    0,
    #     "episode":   3,
    # },
]

# ─────────────────────────────────────────────────────────────────────────────

def build_args(entry: dict) -> list[str]:
    args = [sys.executable, "step_manual_add.py"]

    source = entry["source"]
    if isinstance(source, int):
        args += ["--nyaa-id", str(source)]
    else:
        args.append(str(source))

    if entry.get("title"):
        args += ["--title", entry["title"]]
    if entry.get("no_fankai"):
        args.append("--no-fankai")
    if entry.get("episode") is not None:
        args += ["--episode", str(entry["episode"])]
    if entry.get("season") is not None:
        args += ["--season", str(entry["season"])]
    if entry.get("type"):
        args += ["--type", entry["type"]]

    return args


def main():
    print(f"=== Ajout batch de {len(TORRENTS)} torrent(s) ===\n")

    ok = 0
    errors = 0

    for i, entry in enumerate(TORRENTS, 1):
        source = entry.get("source", "?")
        title  = entry.get("title", "?")
        print(f"[{i:02d}/{len(TORRENTS)}] {title} ← {source}")

        args = build_args(entry)
        result = subprocess.run(args, text=True)

        if result.returncode == 0:
            ok += 1
        else:
            print(f"  [ERR] Code retour : {result.returncode}")
            errors += 1

        print()

    print(f"{'─'*50}")
    print(f"✅ {ok} ajouté(s)  |  ❌ {errors} erreur(s)")


if __name__ == "__main__":
    main()