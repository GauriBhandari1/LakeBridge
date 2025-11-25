from pathlib import Path
import subprocess
import tempfile
import shutil
from datetime import datetime

LAKEBRIDGE_INPUT = Path(__file__).parent.parent / "lakebridge" / "input" / "ssis"
LAKEBRIDGE_OUTPUT = Path(__file__).parent.parent / "lakebridge" / "output" / "ssis"
LAKEBRIDGE_OUTPUT.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd):
    print(f"Running: {cmd}")
    return subprocess.run(cmd, shell=True).returncode == 0

def run():  # <-- rename function to run()
    dtsx_files = list(LAKEBRIDGE_INPUT.glob("*.dtsx"))
    if not dtsx_files:
        print("⚠️ No DTSX files found.")
        return

    for f in dtsx_files:
        print(f"\n🔹 Analyzing {f.name}")
        with tempfile.TemporaryDirectory(dir=LAKEBRIDGE_OUTPUT) as tmp:
            shutil.copy(f, Path(tmp)/f.name)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = LAKEBRIDGE_OUTPUT / f"lakebridge_analysis_{f.stem}_{ts}.xlsx"
            cmd = f'databricks labs lakebridge analyze --source-directory "{tmp}" --report-file "{report_file}" --source-tech SSIS'
            run_cmd(cmd)

    # Transpile step
    converted_folder = LAKEBRIDGE_OUTPUT / "Converted_Code"
    converted_folder.mkdir(exist_ok=True)
    print(f"\n🔹 Transpiling SQLs to {converted_folder}")
    for f in LAKEBRIDGE_INPUT.glob("*.dtsx"):
        sql_file = f.with_suffix(".sql")
        if sql_file.exists():
            cmd = f'databricks labs lakebridge transpile --input-source "{sql_file}" --source-dialect ssis --output-folder "{converted_folder}"'
            run_cmd(cmd)

if __name__ == "__main__":
    run()
