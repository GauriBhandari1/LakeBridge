from pathlib import Path
import pandas as pd

# Input folder is the SSIS output (from previous step)
INPUT_XLSX = Path(__file__).parent.parent / "lakebridge" / "output" / "ssis"
OUTPUT_SQL = INPUT_XLSX  # Save SQLs in same output folder

def get_unique_filename(base_dir, base_name, ext):
    full_path = base_dir / f"{base_name}.{ext}"
    counter = 1
    while full_path.exists():
        full_path = base_dir / f"{base_name}_{counter}.{ext}"
        counter += 1
    return full_path

def run():
    xlsx_files = list(INPUT_XLSX.glob("*.xlsx"))
    if not xlsx_files:
        print("⚠️ No Excel files found for SSIS.")
        return

    for xlsx_file in xlsx_files:
        print(f"\n📄 Processing {xlsx_file.name}")
        try:
            df = pd.read_excel(xlsx_file, sheet_name="SQL Statements", engine='openpyxl', dtype=str)
        except Exception as e:
            print(f"⚠️ Skipping {xlsx_file.name}: {e}")
            continue

        if not all(col in df.columns for col in ["Item Name", "SQL"]):
            print(f"⚠️ Skipping {xlsx_file.name} — missing required columns")
            continue

        package_folder = OUTPUT_SQL / xlsx_file.stem
        package_folder.mkdir(exist_ok=True)

        for _, row in df.iterrows():
            item = str(row["Item Name"]).replace(" ", "_")
            sql_text = str(row["SQL"])
            sql_file = get_unique_filename(package_folder, item, "sql")
            with open(sql_file, "w", encoding="utf-8") as f:
                f.write(sql_text)
            print(f"➤ {sql_file.name}")

if __name__ == "__main__":
    run()
