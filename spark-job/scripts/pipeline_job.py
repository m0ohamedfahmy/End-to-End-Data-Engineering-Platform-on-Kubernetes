import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Paths passed as env vars from SparkApplication so DAG can override them
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://minio-service.minio.svc.cluster.local:9000")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET",    "data-pro")
INPUT_PREFIX    = os.getenv("INPUT_PREFIX",    "raw")
OUTPUT_PREFIX   = os.getenv("OUTPUT_PREFIX",   "processed")
RUN_DATE        = os.getenv("RUN_DATE",        "19700101")  # Unix Epoch - ( 1970-01-01 )

INPUT_PATH  = f"s3a://{MINIO_BUCKET}/{INPUT_PREFIX}/"
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/{OUTPUT_PREFIX}/{RUN_DATE}/"


def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"pipeline-{RUN_DATE}")
        .getOrCreate()
    )


def main():
    print("=" * 60)
    print(f"Pipeline Job  run_date={RUN_DATE}")
    print(f"Input  : {INPUT_PATH}")
    print(f"Output : {OUTPUT_PATH}")
    print("=" * 60)

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------------- Read from data-pro/raw 
    
    print(f"Reading input from {INPUT_PATH}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)          
    )

    print(f"Input row count : {df.count()}")
    print("Input schema:")
    df.printSchema()
    df.show(5, truncate=False)

    # ------------------------------------------------- Transformation 
    # Select only the three columns we care about
    final_df = df.select("price", "seller", "status")

    # Drop rows where all three columns are null
    final_df = final_df.dropna(how="all", subset=["price", "seller", "status"])

    print(f"\nOutput row count : {final_df.count()}")
    print("Output sample:")
    final_df.show(10, truncate=False)

    # -------------------------------------------- Write to data-pro/processed 
    print(f"\nWriting output to {OUTPUT_PATH}")
    (
        final_df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )
    print("Write complete.")

    # --------------------------------------------- Verify write succeeded
    verify = spark.read.parquet(OUTPUT_PATH)
    written = verify.count()
    print(f"Verification: {written} rows written to {OUTPUT_PATH}")
    assert written > 0, f"Output is empty — check input data at {INPUT_PATH}"
    print("✓ Pipeline job finished successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
