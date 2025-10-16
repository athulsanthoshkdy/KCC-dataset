import os
import glob
from pyspark.sql import SparkSession

# Customize this if needed
csv_pattern = "kcc_dataset_*.csv"
output_parquet = "kcc_all_states_combined.parquet"

def colored(msg, color=""):
    # For basic CLI coloring; skip if your terminal does not support
    COLORS = {"red":"\033[91m", "green":"\033[92m", "yellow":"\033[93m", "":"", "end":"\033[0m"}
    return f"{COLORS.get(color,'')}{msg}{COLORS['end']}" if COLORS.get(color) else msg

# 1. Precheck: List all CSVs and their sizes
files = sorted(glob.glob(csv_pattern))
if not files:
    print(colored(f"No files found matching '{csv_pattern}'. Exiting.", "red"))
    exit()

print(colored(f"Found {len(files)} CSV files:", "green"))
for f in files:
    print(f" - {f} ({os.path.getsize(f)/1024/1024:.2f} MB)")

# 2. Identify empty files (skip those)
non_empty_files = [f for f in files if os.path.getsize(f) > 0]
empty_files = [f for f in files if os.path.getsize(f) == 0]
if empty_files:
    print(colored(f"Warning: {len(empty_files)} empty CSV files will be ignored:", "yellow"))
    for f in empty_files:
        print(f"    {f}")

# 3. Create Spark session
spark = SparkSession.builder.appName("KCC BigData Combine").getOrCreate()

# 4. Schema sample check (optional)
schemas = []
for f in non_empty_files[:3]:
    df = spark.read.option("header",True).csv(f)
    schemas.append(df.schema)
    print(colored(f"Schema sample from {f}:", "green"))
    df.printSchema()

all_same = all(str(schemas[0]) == str(s) for s in schemas)
if all_same:
    print(colored("Schemas across sample files match.", "green"))
else:
    print(colored("Warning: Schemas differ across files. You may face issues.", "yellow"))

# 5. Load all non-empty CSVs into one DataFrame
print(colored("Loading all non-empty csv files into a single Spark DataFrame ...", "green"))
df = spark.read.option("header",True).csv(non_empty_files)
print(colored(f"Total records loaded: {df.count()}", "green"))

# 6. Optional deduplication if possible (adjust column names accordingly)
if "QueryID" in df.columns:
    original_count = df.count()
    df = df.dropDuplicates(["QueryID"])
    duplicates = original_count - df.count()
    if duplicates > 0:
        print(colored(f"Removed {duplicates} duplicates based on QueryID column.", "yellow"))

# 7. Write combined DataFrame to Parquet file for faster analytics
print(colored(f"Saving combined data as Parquet file: {output_parquet}", "green"))
df.write.mode("overwrite").parquet(output_parquet)
print(colored("Parquet file created successfully.", "green"))

# 8. Stop Spark session
spark.stop()
