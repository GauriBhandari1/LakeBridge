import argparse
import shutil
import subprocess
import sys
import logging
import yaml
import os  # ✅ Added
from pathlib import Path
from datetime import datetime
import sqlparse
import csv
import urllib.request  # >>> Added


# -------------------------
# Setup Logging (file only)
# -------------------------
def setup_logging(metadata_folder: Path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = metadata_folder / f"lakebridge_run_{ts}.txt"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.info(f"Logging initialized. Log file: {log_file}")
    return log_file


# -------------------------
# Utility functions
# -------------------------
def check_cli():
    if shutil.which("databricks") is None:
        sys.exit("ERROR: 'databricks' CLI not found in PATH. Install/configure it and try again.")


def ensure_dirs(folder: Path):
    folder.mkdir(parents=True, exist_ok=True)


def run_cmd(cmd_str: str, title: str, log_file=None, ignore_failure=False):
    print(f"\n=== {title} ===")
    print("Command:", cmd_str)
    try:
        result = subprocess.run(cmd_str, shell=True, timeout=21600)
        if result.returncode != 0:
            msg = f"{title} failed with exit code {result.returncode}"
            if log_file:
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(msg + "\n")
            if not ignore_failure:
                sys.exit(msg)
            return False
        return True
    except subprocess.TimeoutExpired:
        msg = f"{title} timed out after 21600 seconds/6 hours"
        if log_file:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        if not ignore_failure:
            sys.exit(msg)
        return False


def validate_input_folder(source_path: Path):
    if not source_path.exists():
        sys.exit(f"ERROR: source path not found: {source_path}")
    if not any(source_path.glob("*.sql")):
        print(f"WARNING: No .sql files found in {source_path}")


# -------------------------
# SQL Post-processing + notebooks
# -------------------------
def process_sql_files(converted_folder: Path, notebooks_folder: Path, metadata_folder: Path):
    final_folder = converted_folder.parent / "Final_Formatted"
    ensure_dirs(final_folder)
    ensure_dirs(notebooks_folder)

    summary = []

    print("\nPost-processing SQL and generating notebooks started...")
    for sql_file in converted_folder.glob("*.sql"):
        status = "Succeeded"
        try:
            with open(sql_file, "r", encoding="utf-8", errors="replace") as f:
                sql_content = f.read()

            sql_content = sqlparse.format(sql_content, reindent=True, keyword_case="upper")

            final_file = final_folder / sql_file.name
            with open(final_file, "w", encoding="utf-8") as f:
                f.write(sql_content)

            notebook_file = notebooks_folder / (sql_file.stem + ".py")
            with open(notebook_file, "w", encoding="utf-8") as f:
                f.write("# Databricks notebook source\n")
                f.write(f'"""\nAuto-generated from {sql_file.name}\n"""\n\n')
                f.write('sql_query = """\n')
                f.write(sql_content)
                f.write('\n"""\n')
                f.write("display(spark.sql(sql_query))\n")

            upload_cmd = (
                f'databricks workspace import '
                f'--file "{notebook_file}" '
                f'"/Shared/{notebook_file.name}" '
                f'--language PYTHON --overwrite'
            )
            run_cmd(upload_cmd, f"Upload Notebook {notebook_file.name}",
                    log_file=metadata_folder / f"lakebridge_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    ignore_failure=True)

        except Exception as e:
            status = f"Failed: {e}"
            logging.error(f"Error processing {sql_file.name}: {e}")

        summary.append((sql_file.name, status))

    return summary


# -----------------------------------------------
#  LakeBridge Folder Structure Initialization
# -----------------------------------------------
def create_initial_structure(root_dir: Path = Path("lakebridge")):
    supported_dialects = [
        "abinitio", "adf", "alteryx", "athena", "bigquery", "cloudera_impala",
        "datastage", "greenplum", "hive", "ibm_db2", "ms_sql_server", "netezza",
        "oozie", "oracle", "oracle_data_integrator", "pentahodi", "pig", "presto",
        "pyspark", "redshift", "sap_hana_calcviews", "snowflake", "sp_ss",
        "sqoop", "ssis", "ssrs", "synapse", "talend", "teradata", "vertica"
    ]

    input_dir = root_dir / "input"
    output_dir = root_dir / "output"
    logs_dir = root_dir / "logs"

    for dialect in supported_dialects:
        (input_dir / dialect).mkdir(parents=True, exist_ok=True)
        (output_dir / dialect).mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n✅ LakeBridge base structure created under {root_dir.resolve()}")
    print("Put your SQL scripts inside lakebridge/input/<dialect>/")
    print("Converted scripts will appear under lakebridge/output/<dialect>/")
    print("Then re-run: python runner.py\n")


def is_first_time_setup():
    root_dir = Path("lakebridge")
    return not root_dir.exists() or not any(root_dir.iterdir())


# -------------------------
# Main function
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Run Lakebridge Analyze and Convert (Transpile) commands.")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config file")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file {config_path} not found. Downloading from GitHub...")
        url = "https://raw.githubusercontent.com/satyamhtek/LakeBridge/main/config.yaml"
        urllib.request.urlretrieve(url, config_path)
        print(f"Downloaded default config to {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if is_first_time_setup():
        print("\nDetected first-time setup. Creating LakeBridge folder structure...")
        create_initial_structure()
        print("Setup complete. Exiting now so you can place your SQL scripts.")
        sys.exit(0)

    dialect = config["dialect"]
    source_root = Path(config["source_path"])
    target_root = Path(config["target_path"])

    # Normalize dialect name (lowercase and underscore-safe)
    dialect_folder = dialect.strip().lower().replace(" ", "_")

    # Dynamically build full paths
    source_path = source_root / dialect_folder
    target_path = target_root / dialect_folder

    print(f"\n🔍 Using source folder: {source_path}")
    print(f"📂 Using target folder: {target_path}")

    profile = config.get("profile")
    debug = config.get("debug", False)
    run_validation = config.get("run_validation", True)

    run_analyzer = config.get("run_analyzer", True)
    run_transpiler = config.get("run_transpiler", True)
    if not run_analyzer and not run_transpiler:
        print("\n⚠️ Nothing to run. Both run_analyzer and run_transpiler are False in config.")
        sys.exit(0)

    ts_folder = datetime.now().strftime("%Y%m%d")
    metadata_folder = target_path / "metadata" / ts_folder
    ensure_dirs(metadata_folder)
    log_file = setup_logging(metadata_folder)

    print("\nLakebridge script started\n")
    check_cli()
    if run_validation:
        validate_input_folder(source_path)

    analyzer_output_folder = target_path / "analyzer_output"
    ensure_dirs(analyzer_output_folder)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    analyzer_report_file = analyzer_output_folder / f"lakebridge_analysis_{ts}.xlsx"

    global_flags = []
    if profile:
        global_flags += ["-p", profile]
    if debug:
        global_flags += ["--debug"]

    analyzer_status_dict = {}
    try:
        if run_analyzer:
            analyze_cmd_parts = [
                "databricks labs lakebridge analyze",
                f'--source-directory "{source_path}"',
                f'--report-file "{analyzer_report_file}"',
                f'--source-tech {dialect}',
            ] + global_flags
            run_cmd(" ".join(analyze_cmd_parts), "Lakebridge Analyze", log_file=log_file)
            for sql_file in source_path.glob("*.sql"):
                analyzer_status_dict[sql_file.name] = "Success"
    except Exception as e:
        logging.error(f"Analyzer failed: {e}")
        for sql_file in source_path.glob("*.sql"):
            analyzer_status_dict[sql_file.name] = "Failed"

    converted_folder = target_path / "Converted_Code"
    ensure_dirs(converted_folder)

    transpile_status_dict = {}
    if run_transpiler:
        print("\nStarting transpile per SQL file...")
        for sql_file in source_path.glob("*.sql"):
            try:
                transpile_cmd_parts = [
                    "databricks labs lakebridge transpile",
                    f'--input-source "{sql_file}"',
                    f'--source-dialect {dialect.lower()}',
                    f'--output-folder "{converted_folder}"',
                ] + global_flags
                success = run_cmd(" ".join(transpile_cmd_parts),
                                  f"Transpile {sql_file.name}", log_file=log_file, ignore_failure=True)
                transpile_status_dict[sql_file.name] = "Success" if success else "Failed"
            except Exception as e:
                logging.error(f"Transpile failed for {sql_file.name}: {e}")
                transpile_status_dict[sql_file.name] = "Failed"

    notebooks_folder = target_path / "Databricks_Notebooks"
    post_process_summary = process_sql_files(converted_folder, notebooks_folder, metadata_folder) if run_transpiler else []

    summary_file = metadata_folder / f"sql_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(summary_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Script Name", "Analyzer Status", "Transpile Status", "Post-process Status"])
        all_files = set(list(analyzer_status_dict.keys()) + list(transpile_status_dict.keys()))
        post_process_dict = dict(post_process_summary)
        for file_name in all_files:
            writer.writerow([
                file_name,
                analyzer_status_dict.get(file_name, "Skipped" if not run_analyzer else "Failed"),
                transpile_status_dict.get(file_name, "Skipped" if not run_transpiler else "Failed"),
                post_process_dict.get(file_name, "Skipped" if not run_transpiler else "Failed"),
            ])
    print(f"\nAll tasks completed. Summary CSV saved at {summary_file}")
    sys.exit(0)


# -------------------------
# Entry Point with CURL logic
# -------------------------
if __name__ == "__main__":
    if "CURL_SETUP" in os.environ:
        if is_first_time_setup():
            print("\n🚀 First-time setup detected (CURL mode). Creating LakeBridge folder structure...")
            create_initial_structure()
            print("✅ Setup completed. Exiting now.")
            sys.exit(0)
        else:
            print("✅ LakeBridge folder structure already exists. Skipping setup and exiting.")
            sys.exit(0)
    else:
        main()
