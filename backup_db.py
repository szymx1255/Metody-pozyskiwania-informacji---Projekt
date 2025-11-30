import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Union

def backup_db(db_path: Union[str, Path], backups_dir: Union[str, Path] = "backups", keep: int = 7) -> str:
    backups_dir = Path(backups_dir)
    backups_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    src = Path(db_path)
    dst = backups_dir / f"{src.stem}.{ts}{src.suffix}"
    shutil.copy2(src, dst)
    files = sorted(backups_dir.glob(f"{src.stem}*{src.suffix}"), key=lambda p: p.stat().st_mtime)
    if len(files) > keep:
        for f in files[:len(files)-keep]:
            try:
                f.unlink()
            except Exception:
                pass
    return str(dst)

def restore_db(backup_file: Union[str, Path], target_db: Union[str, Path]) -> None:
    backup_file = Path(backup_file)
    target_db = Path(target_db)
    shutil.copy2(backup_file, target_db)