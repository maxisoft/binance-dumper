from pathlib import Path
import sys
import shutil


if __name__ == '__main__':
    for arg in sys.argv[1:]:
        p = Path(arg)
        for folder in filter(Path.is_dir, p.glob("**/openinterest")):
            if folder.name != "openinterest":
                continue
            target: Path = folder.parent / "openInterest"
            if target != folder:
                for file in folder.glob("*.csv"):
                    shutil.move(str(file), str(target / file.name))
                print(f"moved {folder} -> {target}")
                folder.rmdir()

        for folder in filter(Path.is_dir, p.glob("**/openInterest/openInterest")):
            if folder.name != "openInterest":
                continue
            target: Path = folder.parent
            if target != folder:
                for file in folder.glob("*.csv"):
                    shutil.move(str(file), str(target / file.name))
                print(f"moved {folder} -> {target}")
                folder.rmdir()