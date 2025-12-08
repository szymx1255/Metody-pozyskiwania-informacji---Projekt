# backup_db.py
import shutil
from pathlib import Path
from datetime import datetime
import sqlite3


# Tworzy kopie zapasowa pliku bazy danych z timestampem w nazwie
# Usuwa starsze backupy pozostawiajac tylko okreslona liczbe najnowszych
# Zwraca sciezke do utworzonego pliku backupu
def backup_db(src_path: str | Path, backups_dir: str | Path = "data/backups", keep: int = 7) -> str:
    src = Path(src_path)
    if not src.exists():
        raise FileNotFoundError(f"DB file not found: {src}")
    dst_dir = Path(backups_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    # Utworz timestamp w formacie UTC
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dst = dst_dir / f"{src.name}.{stamp}.bak"
    shutil.copy2(src, dst)
    # Usun stare backupy przekraczajace limit
    files = sorted(dst_dir.glob(f"{src.name}*.bak"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        try:
            old.unlink()
        except Exception:
            pass
    return str(dst)


# Przywraca baze danych z pliku backupu
# Sprawdza integralnosc przywroconej bazy za pomoca PRAGMA integrity_check
# Podnosi RuntimeError jesli sprawdzenie integralnosci sie nie powiedzie
def restore_db(backup_file: str | Path, target_db: str | Path) -> None:
    backup = Path(backup_file)
    target = Path(target_db)
    if not backup.exists():
        raise FileNotFoundError(f"Backup not found: {backup}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(backup, target)
    # Sprawdz integralnosc przywroconej bazy
    conn = sqlite3.connect(str(target))
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check")
        ok = cur.fetchone()
        conn.close()
        if not ok or (isinstance(ok, tuple) and ok[0] != "ok") or (isinstance(ok, str) and ok != "ok"):
            raise RuntimeError(f"DB integrity_check failed: {ok}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    # Interfejs linii polecen do tworzenia i przywracania backupow
    import argparse
    p = argparse.ArgumentParser(prog="backup_db.py")
    p.add_argument("action", choices=["backup","restore"])
    p.add_argument("--src", "-s", default="data/meteo.db", help="Plik bazy źródłowej (do backupu) / backup do przywrócenia")
    p.add_argument("--dst", "-d", default="data/backups", help="Katalog backupów lub docelowa ścieżka przy restore")
    p.add_argument("--keep", type=int, default=7)
    args = p.parse_args()
    if args.action == "backup":
        out = backup_db(args.src, args.dst, keep=args.keep)
        print("Backup utworzony:", out)
    else:
        # Przy restore argument dst jest sciezka docelowa bazy
        restore_db(args.src, args.dst)
        print("Przywrócono backup do:", args.dst)