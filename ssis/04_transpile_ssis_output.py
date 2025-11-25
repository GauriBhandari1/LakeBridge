from pathlib import Path
import subprocess

OUTPUT_SQL = Path(__file__).parent.parent / "lakebridge" / "output" / "ssis"

def run_cmd(cmd):
    print(f"Running: {cmd}")
    return subprocess.run(cmd, shell=True).returncode == 0

def run():
    for folder in OUTPUT_SQL.iterdir():
        if folder.is_dir():
            print(f"\n🔹 Transpiling SQLs in {folder.name}")
            transpiled_folder = folder / "transpiled"
            transpiled_folder.mkdir(exist_ok=True)
            for sql_file in folder.glob("*.sql"):
                cmd = f'databricks labs lakebridge transpile --input-source "{sql_file}" --source-dialect synapse --output-folder "{transpiled_folder}"'
                run_cmd(cmd)

if __name__ == "__main__":
    run()
