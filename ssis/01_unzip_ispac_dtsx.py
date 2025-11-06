import os
import zipfile
import shutil
import tempfile

# --- USER INPUT ---
zip_path = input("Enter the full path of the zip file (.zip): ").strip()
output_dir = input("Enter the directory to save extracted .dtsx files: ").strip()

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Step 1: Create a temp directory for unzipping the outer .zip
with tempfile.TemporaryDirectory() as temp_dir:
    print(f"🔓 Unzipping outer zip: {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as outer_zip:
        outer_zip.extractall(temp_dir)

    print(f"✅ Extracted outer zip to temp folder: {temp_dir}")
    print("🔍 Scanning for .ispac files...")

    # Step 2: Walk through all folders to find .ispac files
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            if file.lower().endswith('.ispac'):
                ispac_path = os.path.join(root, file)
                print(f"📦 Found .ispac: {ispac_path}")

                # Step 3: Open the .ispac as a zip file
                try:
                    with zipfile.ZipFile(ispac_path, 'r') as ispac_zip:
                        for name in ispac_zip.namelist():
                            if name.lower().endswith('.dtsx'):
                                # Extract and save with unique filename
                                base_name = os.path.splitext(file)[0]
                                dtsx_name = os.path.basename(name)
                                dest_file = f"{base_name}_{dtsx_name}"

                                # Ensure uniqueness
                                counter = 1
                                final_path = os.path.join(output_dir, dest_file)
                                while os.path.exists(final_path):
                                    dest_file = f"{base_name}_{counter}_{dtsx_name}"
                                    final_path = os.path.join(output_dir, dest_file)
                                    counter += 1

                                with ispac_zip.open(name) as dtsx_file, open(final_path, 'wb') as out_file:
                                    shutil.copyfileobj(dtsx_file, out_file)

                                print(f"    ➤ Extracted: {dest_file}")

                except zipfile.BadZipFile:
                    print(f"❌ Skipping invalid .ispac (not a zip): {ispac_path}")

print(f"\n🎉 Done! All .dtsx files are in: {output_dir}")
