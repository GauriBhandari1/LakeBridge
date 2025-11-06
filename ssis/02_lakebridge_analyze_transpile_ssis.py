import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import sqlparse
import tempfile

def check_cli():
    if shutil.which("databricks") is None:
        sys.exit("ERROR: 'databricks' CLI not found in PATH. Install/configure it and try again.")

def ensure_dirs(folder: Path):
    folder.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd_str: str, title: str, capture_output=False):
    print(f"\n=== {title} ===")
    print("Command:", cmd_str)
    try:
        result = subprocess.run(cmd_str, shell=True, timeout=1800, capture_output=capture_output, text=True)
        if result.returncode != 0:
            if capture_output:
                return False, result.stdout, result.stderr
            else:
                sys.exit(f"{title} failed with exit code {result.returncode}")
        if capture_output:
            return True, result.stdout, result.stderr
        return True, None, None
    except subprocess.TimeoutExpired:
        sys.exit(f"{title} timed out after 1800 seconds")

def process_sql_files(converted_folder: Path, notebooks_folder: Path):
    final_folder = converted_folder.parent / "Final_Formatted"
    ensure_dirs(final_folder)
    ensure_dirs(notebooks_folder)

    for sql_file in converted_folder.glob("*.sql"):
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Format SQL
        sql_content = sqlparse.format(sql_content, reindent=True, keyword_case="upper")

        # Save formatted SQL
        final_file = final_folder / sql_file.name
        with open(final_file, "w", encoding="utf-8") as f:
            f.write(sql_content)

        # Create Databricks notebook (.py)
        notebook_file = notebooks_folder / (sql_file.stem + ".py")
        with open(notebook_file, "w", encoding="utf-8") as f:
            f.write("# Databricks notebook source\n")
            f.write(f'"""\nAuto-generated from {sql_file.name}\n"""\n\n')
            f.write('sql_query = """\n')
            f.write(sql_content)
            f.write('\n"""\n')
            f.write("display(spark.sql(sql_query))\n")

        # Upload notebook
        upload_cmd = (
            f'databricks workspace import '
            f'--file "{notebook_file}" '
            f'"/Shared/{notebook_file.name}" '
            f'--language PYTHON --overwrite'
        )
        run_cmd(upload_cmd, f"Upload Notebook {notebook_file.name}")

def main():
    parser = argparse.ArgumentParser(description="Run Lakebridge Analyze and Convert (Transpile) commands.")
    parser.add_argument("--source-path", help="Folder (or file) used for analyze/transpile --source-directory")
    parser.add_argument("--target-path", help="Folder where analyzer report and converted code will be saved")
    parser.add_argument("--dialect", help="Source dialect/tech (e.g., synapse, oracle, teradata)")
    parser.add_argument("--profile", help="Optional Databricks CLI profile name (maps to -p/--profile)")
    parser.add_argument("--debug", action="store_true", help="Enable Lakebridge debug logging (--debug)")
    args = parser.parse_args()

    source_path = args.source_path or input('Enter source path (folder or file): ').strip('"').strip()
    target_path = args.target_path or input('Enter target folder: ').strip('"').strip()
    dialect_input = args.dialect or input('Enter source dialect/tech (e.g., synapse, oracle, teradata): ').strip()

    target_folder = Path(target_path)
    ensure_dirs(target_folder)

    check_cli()
    if not Path(source_path).exists():
        sys.exit(f"ERROR: source path not found: {source_path}")

    global_flags = []
    if args.profile:
        global_flags += ["-p", args.profile]
    if args.debug:
        global_flags += ["--debug"]

    # Normalize dialect for transpile (lowercase)
    dialect_transpile = dialect_input.lower()

    source_path_obj = Path(source_path)
    dtsx_files = []
    if source_path_obj.is_file():
        # If user gave a single file, only that
        if source_path_obj.suffix.lower() == ".dtsx":
            dtsx_files = [source_path_obj]
        else:
            sys.exit("ERROR: Provided file is not a .dtsx file.")
    else:
        # Folder, get all .dtsx files inside
        dtsx_files = list(source_path_obj.glob("*.dtsx"))
        if not dtsx_files:
            sys.exit("ERROR: No .dtsx files found in the source folder.")

    # 1️⃣ Run Analyzer separately for each dtsx file by copying into temp dir
    for dtsx_file in dtsx_files:
        print(f"\n=== Lakebridge Analyze {dtsx_file.name} ===")

        with tempfile.TemporaryDirectory(dir=target_folder) as temp_dir:
            temp_path = Path(temp_dir)
            shutil.copy(dtsx_file, temp_path / dtsx_file.name)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = target_folder / f"lakebridge_analysis_{dtsx_file.stem}_{ts}.xlsx"

            analyze_cmd_parts = [
                "databricks labs lakebridge analyze",
                f'--source-directory "{temp_path}"',
                f'--report-file "{report_file}"',
                f'--source-tech SSIS',  # Hardcoded exactly as SSIS to avoid prompt
            ] + global_flags

            run_cmd(" ".join(analyze_cmd_parts), f"Lakebridge Analyze {dtsx_file.name}")

    # 2️⃣ Run Converter (Transpile) for all SQL files after analyze completes
    converted_folder = target_folder / "Converted_Code"
    ensure_dirs(converted_folder)

    # Find all .sql files in source_path or converted_folder for transpile
    # Assuming dtsx files transpile to SQL elsewhere, we look for .sql files in source_path folder
    sql_files = []
    for dtsx_file in dtsx_files:
        sql_candidate = dtsx_file.with_suffix(".sql")
        if sql_candidate.exists():
            sql_files.append(sql_candidate)
    # If none found by that, fallback to all sql in source folder
    if not sql_files:
        sql_files = list(source_path_obj.glob("*.sql"))

    successful_transpiles = 0
    for sql_file in sql_files:
        print(f"\n=== Transpile {sql_file.name} ===")
        transpile_cmd = (
            f'databricks labs lakebridge transpile '
            f'--input-source "{sql_file}" '
            f'--source-dialect {dialect_transpile} '
            f'--output-folder "{converted_folder}" '
        )
        if global_flags:
            transpile_cmd += " " + " ".join(global_flags)

        success, stdout, stderr = run_cmd(transpile_cmd, f"Transpile {sql_file.name}", capture_output=True)
        if not success:
            print(f"⚠️ Transpile failed for {sql_file.name}: {stderr.strip()}")
        else:
            successful_transpiles += 1
            print(stdout.strip())

    if successful_transpiles == 0:
        print("WARNING: No files transpiled successfully.")

    # 3️⃣ Post-process SQL + Generate Notebooks
    notebooks_folder = target_folder / "Databricks_Notebooks"
    process_sql_files(converted_folder, notebooks_folder)

    print("\nAll done ✅")
    print(f"Analyzer reports saved in: {target_folder}")
    print(f"Converted scripts saved at: {converted_folder}")
    print(f"Final formatted scripts saved at: {converted_folder.parent / 'Final_Formatted'}")
    print(f"Databricks notebooks saved at: {notebooks_folder} and uploaded to /Shared in workspace")

if __name__ == "__main__":
    main()

