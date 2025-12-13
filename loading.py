# loading.py 
from dotenv import load_dotenv
import os
from pathlib import Path
import psycopg2
from pyspark.sql import SparkSession

# Load .env file
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

DB_NAME = os.getenv("F4F_DB_NAME", "F4F_DB")
DB_USER = os.getenv("F4F_DB_USER", "postgres")
DB_PASSWORD = os.getenv("F4F_DB_PASSWORD") 
DB_HOST = os.getenv("F4F_DB_HOST", "host.docker.internal")
# DB_HOST = os.getenv("F4F_DB_HOST", "localhost")
DB_PORT = os.getenv("F4F_DB_PORT", "5432")

if not DB_PASSWORD:
    raise ValueError("Environment variable F4F_DB_PASSWORD not set!")


def get_db_connection():
    """Postgres connection for running DDL."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()

    create_table_query = '''
   
                        CREATE SCHEMA IF NOT EXISTS f4f;

                        DROP TABLE IF EXISTS f4f.dim_business CASCADE;
                        DROP TABLE IF EXISTS f4f.dim_rider CASCADE;
                        DROP TABLE IF EXISTS f4f.dim_flour CASCADE;
                        DROP TABLE IF EXISTS f4f.fact_orders CASCADE;

                        CREATE TABLE f4f.dim_business (
                            business_id VARCHAR PRIMARY KEY,
                            business_name VARCHAR NOT NULL,
                            business_type VARCHAR,
                            business_address VARCHAR,
                            contact_name VARCHAR NOT NULL,
                            contact_phone VARCHAR NOT NULL
                            
                        );

                        CREATE TABLE f4f.dim_rider (
                            rider_id SERIAL PRIMARY KEY,
                            rider_name VARCHAR NOT NULL,
                            rider_phone VARCHAR NOT NULL
                        );

                        CREATE TABLE f4f.dim_flour (
                            flour_type_id SERIAL PRIMARY KEY,
                            flour_type VARCHAR(10000)
                            
                        );

                        CREATE TABLE f4f.fact_orders (
                            order_id       VARCHAR PRIMARY KEY,
                            order_date     DATE NOT NULL,
                            delivery_date  DATE NOT NULL,
                            business_id    VARCHAR NOT NULL REFERENCES f4f.dim_business(business_id),
                            rider_id       INT NOT NULL REFERENCES f4f.dim_rider(rider_id),
                            flour_type_id  INT NOT NULL REFERENCES f4f.dim_flour(flour_type_id),
                            quantity_bags  INT NOT NULL,
                            price_per_bag   NUMERIC NOT NULL,
                            total_amount   NUMERIC NOT NULL,
                            payment_method VARCHAR NOT NULL,
                            order_status   VARCHAR NOT NULL
                        );
    '''

    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully.")


def run_loading():
    print("Starting load step for Flour4Four...")

    # Start the Spark session
    PROJECT_ROOT = Path(__file__).resolve().parent
    CLEANED_DATA_DIR = PROJECT_ROOT / "clean_data_parquet"

    spark = (
        SparkSession.builder
        .appName("Flour4Four_PySpark_Load")
        .config("spark.jars", str(PROJECT_ROOT / "postgresql-42.7.8.jar"))
        .getOrCreate()
    )

    # load the parquet file
    
    dim_business = spark.read.parquet(str(CLEANED_DATA_DIR / "dim_business"))
    dim_rider = spark.read.parquet(str(CLEANED_DATA_DIR / "dim_rider"))
    dim_flour = spark.read.parquet(str(CLEANED_DATA_DIR / "dim_flour"))
    fact_orders = spark.read.parquet(str(CLEANED_DATA_DIR / "fact_orders"))

    print("Parquet dimension and fact tables loaded successfully.")

    # create tables
    create_table()

    # JDBC URL
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    jdbc_props = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    
    # loading data into table
    dim_business.write.jdbc(url=jdbc_url, table="f4f.dim_business", mode="append", properties=jdbc_props)
    dim_rider.write.jdbc(url=jdbc_url, table="f4f.dim_rider", mode="append", properties=jdbc_props)
    dim_flour.write.jdbc(url=jdbc_url, table="f4f.dim_flour", mode="append", properties=jdbc_props)
    fact_orders.write.jdbc(url=jdbc_url, table="f4f.fact_orders", mode="append", properties=jdbc_props)
    

    print("Data successfully loaded into PostgreSQL.")

    spark.stop()


if __name__ == "__main__":
    run_loading()
