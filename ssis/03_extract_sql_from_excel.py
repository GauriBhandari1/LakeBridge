import os
import pandas as pd

# --- USER INPUT ---
input_folder = input("Enter the path to the folder containing the Excel (.xlsx) files: ").strip('"').strip()
output_root_folder = input("Enter the path to the output folder where SQL files should be saved: ").strip('"').strip()

# Validate input folder
if not os.path.exists(input_folder):
    print(f"❌ ERROR: Input folder not found: {input_folder}")
    exit(1)

# Create output root folder if it doesn't exist
os.makedirs(output_root_folder, exist_ok=True)

# Helper: Create unique filename in case of name conflicts
def get_unique_filename(base_dir, base_name, ext):
    full_path = os.path.join(base_dir, f"{base_name}.{ext}")
    counter = 1
    while os.path.exists(full_path):
        full_path = os.path.join(base_dir, f"{base_name}_{counter}.{ext}")
        counter += 1
    return full_path

# Process each Excel file
for filename in os.listdir(input_folder):
    if filename.endswith(".xlsx"):
        file_path = os.path.join(input_folder, filename)
        print(f"\n📄 Processing file: {filename}")

        # Try to read the "SQL Statements" sheet
        try:
            df = pd.read_excel(file_path, sheet_name="SQL Statements", engine='openpyxl', dtype=str)
        except Exception as e:
            print(f"  ⚠️ Skipping (can't read 'SQL Statements'): {e}")
            continue

        # Check required columns
        if not all(col in df.columns for col in ["Item Name", "SQL"]):
            print(f"  ⚠️ Skipping (missing 'Item Name' or 'SQL' columns)")
            continue

        # Create subfolder for this DTSX (xlsx) file
        xlsx_name = os.path.splitext(filename)[0]
        package_output_folder = os.path.join(output_root_folder, xlsx_name)
        os.makedirs(package_output_folder, exist_ok=True)

        # Write each SQL block to a separate file
        for idx, row in df.iterrows():
            item_name = str(row.get("Item Name", "")).strip()
            sql = str(row.get("SQL", "")).strip()

            if item_name and sql:
                # Clean the item name to use in filename
                safe_item_name = item_name.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "_")
                filename_base = f"{safe_item_name}"

                # Get unique file path in the subfolder
                sql_path = get_unique_filename(package_output_folder, filename_base, "sql")

                # Write to .sql file
                with open(sql_path, "w", encoding="utf-8") as f:
                    f.write(sql)

                print(f"    ✅ Extracted SQL to: {sql_path}")

print("\n🎉 All done! SQLs saved per DTSX package folder.")
