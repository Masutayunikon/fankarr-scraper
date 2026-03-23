"""
ÉTAPE 3 - Fankai Verify
========================
Vérifie les fichiers matched/ et affiche un rapport par série.
Par défaut, affiche uniquement les séries avec des problèmes.

Usage :
  python step3_verify.py                     # séries avec problèmes uniquement
  python step3_verify.py --all               # toutes les séries
  python step3_verify.py --serie "Demon Slayer"
"""

import json
import argparse
from pathlib import Path

MATCHED_DIR = Path("series")

R    = "\033[91m"
G    = "\033[92m"
Y    = "\033[93m"
C    = "\033[96m"
W    = "\033[97m"
DIM  = "\033[2m"
RST  = "\033[0m"
BOLD = "\033[1m"

def short_path(p):
    if not p: return ""
    if isinstance(p, dict): p = p.get("path") or ""
    return Path(p).name if p else ""

def ep_status(ep, s_torrents, season_torrents):
    """Retourne (found, has_path, issues)."""
    issues = []
    ep_torrents = ep.get("torrents") or []
    ep_paths    = ep.get("paths") or []
    inherited   = bool(s_torrents or season_torrents)
    has_path    = any((obj.get("path") if isinstance(obj, dict) else obj) for obj in ep_paths)
    found       = bool(ep_torrents) or inherited

    if ep_torrents:
        if len(ep_paths) != len(ep_torrents):
            issues.append(f"⚠ {len(ep_torrents)} torrent(s) mais {len(ep_paths)} path(s)")
        for i, obj in enumerate(ep_paths):
            p = obj.get("path") if isinstance(obj, dict) else obj
            if not p:
                issues.append(f"⚠ path[{i}] est null")

    return found, has_path, issues


def serie_has_problems(data):
    """Retourne True si la série a au moins un problème visible."""
    s_torrents = data.get("torrents") or []
    for season in data.get("seasons") or []:
        st = season.get("torrents") or []
        for ep in season["episodes"]:
            if not ep.get("aired"):
                continue
            _, has_path, issues = ep_status(ep, s_torrents, st)
            if issues:
                return True
            if not has_path and not s_torrents and not st and not (ep.get("torrents")):
                return True
            if (s_torrents or st) and not has_path:
                return True
    return False


def check_serie(data, show_all=False):
    title      = data.get("title", "?")
    s_torrents = data.get("torrents") or []
    seasons    = data.get("seasons") or []

    total_aired   = sum(1 for s in seasons for e in s["episodes"] if e.get("aired"))
    total_eps     = sum(len(s["episodes"]) for s in seasons)
    found_count   = 0
    path_count    = 0
    missing_count = 0
    issue_count   = 0

    for season in seasons:
        st = season.get("torrents") or []
        for ep in season["episodes"]:
            found, has_path, issues = ep_status(ep, s_torrents, st)
            if found:   found_count += 1
            if has_path: path_count += 1
            if ep.get("aired") and not has_path and not s_torrents and not st:
                missing_count += 1
            issue_count += len(issues)

    # Statut global
    if s_torrents and path_count == total_eps:
        status = f"{G}📦 INTÉGRALE ({len(s_torrents)}){RST}"
    elif s_torrents and path_count < total_eps:
        missing_in_pack = total_eps - path_count
        status = f"{Y}📦 INTÉGRALE — {missing_in_pack} paths manquants{RST}"
    elif missing_count == 0 and issue_count == 0 and total_aired > 0:
        status = f"{G}✅ COMPLET{RST}"
    elif found_count == 0 and total_aired > 0:
        status = f"{R}✗ AUCUN{RST}"
    else:
        status = f"{Y}⚠ PARTIEL ({missing_count} manquants){RST}"

    print(f"\n{BOLD}{W}{title}{RST}  {status}")

    for tor in s_torrents:
        print(f"  {C}↳ {tor.get('title', '?')[:80]}{RST}")

    for season in seasons:
        sn     = season.get("season_number", "?")
        stitle = season.get("title", "")
        label  = f"S{sn:02d}" if isinstance(sn, int) else f"S{sn}"
        st     = season.get("torrents") or []

        # En mode non-all, skipper les saisons sans problème
        if not show_all:
            season_has_issue = any(
                ep_status(ep, s_torrents, st)[2] or
                (ep.get("aired") and not ep_status(ep, s_torrents, st)[1])
                for ep in season["episodes"]
            )
            if not season_has_issue:
                continue

        print(f"  {DIM}{label} — {stitle}{RST}")

        for tor in st:
            print(f"    {C}📦 {tor.get('title', '?')[:70]}{RST}")

        for ep in season["episodes"]:
            en     = ep.get("episode_number", "?")
            etitle = ep.get("title", "")
            aired  = ep.get("aired")
            found, has_path, issues = ep_status(ep, s_torrents, st)
            ep_torrents = ep.get("torrents") or []
            ep_paths    = ep.get("paths") or []

            if issues:
                print(f"    {Y}⚠ {RST} {label}E{en:02d} {W}{etitle}{RST}")
                for iss in issues:
                    print(f"       {Y}{iss}{RST}")
                for i, p in enumerate(ep_paths):
                    tor_title = ep_torrents[i].get("title", "?") if i < len(ep_torrents) else "pack"
                    print(f"       [{i}] {DIM}{short_path(p) or 'null'}{RST}  ← {DIM}{tor_title[:40]}{RST}")
            elif has_path:
                if show_all:
                    if ep_torrents and len(ep_torrents) > 1:
                        print(f"    {G}✅{RST} {label}E{en:02d} {DIM}{etitle}{RST} ({len(ep_torrents)} versions)")
                        for i, (tor, p) in enumerate(zip(ep_torrents, ep_paths)):
                            print(f"       [{i}] {DIM}{short_path(p)}{RST}")
                    elif ep_torrents:
                        print(f"    {G}✅{RST} {label}E{en:02d} {DIM}{etitle}{RST}")
                        if ep_paths: print(f"       {DIM}{short_path(ep_paths[0])}{RST}")
                    elif s_torrents or st:
                        print(f"    {G}✅{RST} {label}E{en:02d} {DIM}{etitle}{RST}")
                        if ep_paths: print(f"       {DIM}{short_path(ep_paths[0])}{RST}")
            elif not has_path:
                if not aired:
                    if show_all:
                        print(f"    {DIM}⏳  {label}E{en:02d} {etitle}{RST}")
                elif s_torrents or st:
                    print(f"    {Y}〰 {RST} {label}E{en:02d} {W}{etitle}{RST} {DIM}(path manquant dans le pack){RST}")
                else:
                    print(f"    {R}❌{RST} {label}E{en:02d} {W}{etitle}{RST}")

    return total_aired, missing_count, issue_count


def main():
    parser = argparse.ArgumentParser(description="Vérification des matchings Fankai")
    parser.add_argument("--all",   action="store_true", help="Afficher toutes les séries (y compris les complètes)")
    parser.add_argument("--serie", type=str, default=None, help="Filtrer sur une série")
    args = parser.parse_args()

    files = sorted(MATCHED_DIR.glob("*.json"))
    if not files:
        print(f"{R}Aucun fichier dans {MATCHED_DIR}/{RST}"); return

    if args.serie:
        query = args.serie.lower()
        files = [f for f in files if query in f.stem.lower()
                 or query in f.read_text(encoding="utf-8").lower()[:300]]

    total_aired    = 0
    total_missing  = 0
    total_issues   = 0
    series_ok      = 0
    series_ko      = 0
    series_partial = 0

    for fpath in files:
        try:
            data = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"{R}Erreur {fpath.name}: {e}{RST}"); continue

        s_torrents = data.get("torrents") or []
        seasons    = data.get("seasons") or []
        aired_eps  = sum(1 for s in seasons for ep in s["episodes"] if ep.get("aired"))

        # Stats globales
        has_any_path = any(ep.get("paths") for s in seasons for ep in s["episodes"])
        has_problems = serie_has_problems(data)

        if not has_problems and aired_eps > 0:
            series_ok += 1
        elif not has_any_path and not s_torrents and aired_eps > 0:
            series_ko += 1
        elif aired_eps > 0:
            series_partial += 1

        # Affichage
        if args.all or args.serie or has_problems:
            aired, missing, issues = check_serie(data, show_all=args.all or bool(args.serie))
            total_aired   += aired
            total_missing += missing
            total_issues  += issues
        else:
            total_aired += aired_eps

    print(f"\n{'─'*60}")
    print(f"{BOLD}📊 RÉSUMÉ{RST}")
    print(f"  Séries analysées   : {len(files)}")
    print(f"  {G}✅ Complètes{RST}        : {series_ok}")
    print(f"  {Y}⚠  Avec problèmes{RST}  : {series_partial}")
    print(f"  {R}✗  Aucun torrent{RST}   : {series_ko}")
    print(f"  Épisodes sortis    : {total_aired}")
    if total_missing:
        print(f"  {R}Épisodes manquants : {total_missing}{RST}")
    if total_issues:
        print(f"  {Y}Incohérences       : {total_issues}{RST}")
    coverage = int((total_aired - total_missing) / total_aired * 100) if total_aired else 0
    bar = "█" * (coverage // 5) + "░" * (20 - coverage // 5)
    print(f"  Couverture         : {G if coverage > 80 else Y}{coverage}% {bar}{RST}")
    print()

if __name__ == "__main__":
    main()