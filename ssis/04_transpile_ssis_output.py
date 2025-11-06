# import subprocess
# import logging
# import yaml
# from pathlib import Path
# from datetime import datetime
# from hashlib import md5
# import argparse
# import sys

# # === Logging Setup ===
# def setup_logging(log_folder: Path):
#     log_folder.mkdir(parents=True, exist_ok=True)
#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#     log_file = log_folder / f"transpile_folders_log_{ts}.txt"
#     logging.basicConfig(
#         filename=log_file,
#         level=logging.INFO,
#         format="%(asctime)s [%(levelname)s] %(message)s",
#         encoding="utf-8"
#     )
#     return log_file

# # === Load config.yaml ===
# def load_config(config_path: Path):
#     if not config_path.exists():
#         sys.exit(f"❌ Config file not found: {config_path}")
#     with open(config_path, "r", encoding="utf-8") as f:
#         return yaml.safe_load(f)

# # === Run Command ===
# def run_cmd(cmd_str: str):
#     print(f"▶ {cmd_str}")
#     result = subprocess.run(cmd_str, shell=True)
#     return result.returncode == 0

# # === Unique Output Filename ===
# def make_unique_name(file_path: Path, base_folder: Path) -> str:
#     rel_path = file_path.relative_to(base_folder)
#     hash_suffix = md5(str(rel_path).encode()).hexdigest()[:6]
#     return f"{rel_path.stem}_{hash_suffix}.sql"

# # === Transpile All SQL Files in a Folder ===
# def transpile_sql_folder(sql_folder: Path, source_base: Path, target_base: Path, dialect: str, global_flags: list):
#     sql_files = list(sql_folder.glob("*.sql"))
#     if not sql_files:
#         print(f"⚠️ No SQL files in {sql_folder}")
#         return

#     print(f"\n📁 Transpiling folder: {sql_folder}")
#     for sql_file in sql_files:
#         output_name = make_unique_name(sql_file, source_base)
#         transpile_cmd_parts = [
#             "databricks labs lakebridge transpile",
#             f'--input-source "{sql_file}"',
#             f'--source-dialect {dialect}',
#             f'--output-folder "{target_base}"',
#             f'--output-name "{output_name}"',
#         ] + global_flags

#         cmd = " ".join(transpile_cmd_parts)
#         success = run_cmd(cmd)
#         if success:
#             logging.info(f"✅ Transpiled: {sql_file} → {output_name}")
#         else:
#             logging.error(f"❌ Failed to transpile: {sql_file}")


# # === Main Entry ===
# def main():
#     parser = argparse.ArgumentParser(description="Transpile SQL scripts per DTSX folder using config.yaml")
#     parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
#     args = parser.parse_args()

#     config = load_config(Path(args.config))
#     source_path = Path(config["source_path"])
#     target_path = Path(config["target_path"])
#     dialect = config.get("dialect", "synapse")
#     profile = config.get("profile", None)
#     debug = config.get("debug", False)

#     # Prepare log and output folders
#     log_file = setup_logging(target_path / "logs")
#     target_path.mkdir(parents=True, exist_ok=True)

#     global_flags = []
#     if profile:
#         global_flags += ["--profile", profile]
#     if debug:
#         global_flags += ["--debug"]

#     # Iterate all immediate subfolders in source_path
#     print(f"\n🚀 Starting transpilation from base folder: {source_path}")
#     for folder in source_path.iterdir():
#         if folder.is_dir():
#             transpile_sql_folder(folder, source_path, target_path, dialect, global_flags)

#     print(f"\n✅ Transpilation complete. Logs saved to: {log_file}")


# if __name__ == "__main__":
#     main()



# 04_transpile_ssis_output.py

# 04_transpile_ssis_output.py

import os
import subprocess
import sys
from pathlib import Path
import yaml
from datetime import datetime

def run_cmd(cmd_str):
    print(f"Running: {cmd_str}")
    try:
        result = subprocess.run(cmd_str, shell=True, timeout=600)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"❌ Command timed out: {cmd_str}")
        return False

def transpile_sql_files_in_folder(folder: Path, profile: str, debug: bool):
    transpiled_folder = folder / "transpiled"
    transpiled_folder.mkdir(exist_ok=True)

    for sql_file in folder.glob("*.sql"):
        output_cmd = [
            "databricks labs lakebridge transpile",
            f'--input-source "{sql_file}"',
            "--source-dialect synapse",  # ✅ Always synapse here
            f'--output-folder "{transpiled_folder}"'
        ]

        if profile:
            output_cmd += ["-p", profile]
        if debug:
            output_cmd.append("--debug")

        cmd_str = " ".join(output_cmd)
        success = run_cmd(cmd_str)
        status = "✅" if success else "❌"
        print(f"{status} {sql_file.name}")

def main():
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("❌ config.yaml not found.")
        sys.exit(1)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    target_path = Path(config.get("target_path", ""))
    if not target_path.exists():
        print(f"❌ Target path not found: {target_path}")
        sys.exit(1)

    profile = config.get("profile", "")
    debug = config.get("debug", False)

    print(f"📂 Scanning for DTSX SQL folders in: {target_path}")

    dtsx_folders = [f for f in target_path.iterdir() if f.is_dir()]
    if not dtsx_folders:
        print("⚠️ No DTSX package folders found.")
        sys.exit(0)

    for folder in dtsx_folders:
        print(f"\n🔁 Transpiling SQLs in: {folder.name}")
        transpile_sql_files_in_folder(folder, profile, debug)

    print("\n🎉 Final SSIS SQL transpilation complete.")

if __name__ == "__main__":
    main()
