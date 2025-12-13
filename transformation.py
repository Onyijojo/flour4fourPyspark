# transformation.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pathlib import Path


def run_transformation():
    print("Starting transformation for Flour4Four...")

    # Root project directory
    PROJECT_ROOT = Path(__file__).resolve().parent
    JAR_PATH = PROJECT_ROOT / "postgresql-42.7.8.jar"
    CLEANED_DATA_DIR = PROJECT_ROOT / "clean_data_parquet"

    # Start Spark
    spark = (
            SparkSession.builder
            .appName("Flour4Four_PySpark_Transform")
            .config("spark.jars", str(JAR_PATH))
            .getOrCreate()
        )

    # Load cleaned parquet from extraction step ----------
    cleaned_parquet_path = CLEANED_DATA_DIR / "cleaned_raw_data"

    f4f_df_clean = spark.read.parquet(str(cleaned_parquet_path))

    print("Cleaned data loaded successfully.")

     
    # create business dimension model 
    dim_business = f4f_df_clean.select('business_id', 'business_name', 'business_type', 'business_address', \
                                    'contact_name', 'contact_phone') \
                                    .dropDuplicates(["business_id"])

    
    
    # create dimension table for rider
    dim_rider = f4f_df_clean.select('rider_name', 'rider_phone').dropDuplicates()  \
                        .withColumn('rider_id', monotonically_increasing_id()) \
                        .select('rider_id','rider_name', 'rider_phone')
    

    # create flour dimension table
    dim_flour = f4f_df_clean.select('flour_type').dropDuplicates() \
                        .withColumn('flour_type_id', monotonically_increasing_id()) \
                        .select('flour_type_id', 'flour_type')

    
    # create fact table
    fact_orders = f4f_df_clean.join(dim_flour, ['flour_type'], 'left') \
                            .join(dim_rider, ['rider_name', 'rider_phone'], 'left') \
                            .select('order_id', 'order_date', 'delivery_date', 'business_id', 'rider_id', 'flour_type_id', \
                                    'quantity_bags', 'price_per_bag', 'total_amount', 'payment_method', 'order_status')

    
    # Save dimensions and fact tables as Parquet

    dim_business.write.mode("overwrite").parquet(str(CLEANED_DATA_DIR / "dim_business"))
    dim_rider.write.mode("overwrite").parquet(str(CLEANED_DATA_DIR / "dim_rider"))
    dim_flour.write.mode("overwrite").parquet(str(CLEANED_DATA_DIR / "dim_flour"))
    fact_orders.write.mode("overwrite").parquet(str(CLEANED_DATA_DIR / "fact_orders")    )

    print("All dimension and fact tables exported to folders successfully.")

    spark.stop()


if __name__ == "__main__":
    run_transformation()
