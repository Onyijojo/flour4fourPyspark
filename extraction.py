# extraction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce
from pathlib import Path
from pyspark.sql import functions as F


def run_extraction():
    # Folder where script lives
    PROJECT_ROOT = Path(__file__).resolve().parent
    JAR_PATH = PROJECT_ROOT / "postgresql-42.7.8.jar" 


    RAW_DATA_DIR = PROJECT_ROOT / "raw_data"
    CLEANED_DATA_DIR = PROJECT_ROOT / "clean_data_parquet"
    
    # Ensure directory exists
    CLEANED_DATA_DIR.mkdir(exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Flour4Four_PySpark_Extract")
        .config("spark.jars", str(JAR_PATH))
        .getOrCreate()
    )
   
    print("Extracting flour4four data...")



    # Load raw CSV
    raw_csv_path = RAW_DATA_DIR / "flour4four_orders.csv"
    f4f_df = spark.read.csv(
        str(raw_csv_path),
        header=True,
        inferSchema=True,
        nullValue="None"
    )

    print("Data Loaded Successfully")

    # -------- HANDLE MISSING RECORDS --------

    # Replace null order_date with delivery_date
    f4f_df = f4f_df.withColumn(
        "order_date",
        coalesce(col("order_date"), col("delivery_date"))
    )

    # Fill string columns
    f4f_df = f4f_df.fillna({
        "business_name": "Unknown",
        "business_type": "Unknown",
        "business_address": "Unknown",
        "flour_type": "Unknown",
    })

    # Fill price_per_bag with median
    median_price = f4f_df.approxQuantile("price_per_bag", [0.5], 0.01)[0]
    f4f_df = f4f_df.fillna({"price_per_bag": median_price})

    print("Missing Records Handled Successfully!")

    # -------- SAVE CLEANED DATA AS PARQUET --------

    cleaned_parquet_path = CLEANED_DATA_DIR / "cleaned_raw_data"

    f4f_df.coalesce(1).write.mode("overwrite").parquet("clean_data_parquet/cleaned_raw_data")

    print(f"Cleaned parquet created at: {cleaned_parquet_path}")


# Allow script to run from terminal
if __name__ == "__main__":
    run_extraction()
