"""Retail sales ETL: Extract (CSV) -> Transform (clean + aggregate) -> Load (Parquet)."""
from pyspark.sql import SparkSession, functions as F


def extract(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def transform(df):
    cleaned = (
        df.dropDuplicates()                                   # remove exact duplicate rows
          .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))  # drop bad quantities
          .withColumn("city", F.initcap(F.trim(F.col("city"))))  # " guntur " -> "Guntur"
          .withColumn("order_date", F.to_date("order_date"))
          .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
          .withColumn("month", F.date_format("order_date", "yyyy-MM"))
    )
    monthly = cleaned.groupBy("month").agg(
        F.sum("revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("orders"),
    ).orderBy("month")
    by_category = cleaned.groupBy("category").agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("units_sold"),
    ).orderBy(F.desc("total_revenue"))
    by_city = cleaned.groupBy("city").agg(
        F.sum("revenue").alias("total_revenue")
    ).orderBy(F.desc("total_revenue"))
    return cleaned, monthly, by_category, by_city


def load(df, path):
    df.write.mode("overwrite").parquet(path)


def main():
    spark = SparkSession.builder.appName("RetailSalesETL").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = extract(spark, "sales_raw.csv")
    print(f"Raw rows: {raw.count()}")

    cleaned, monthly, by_category, by_city = transform(raw)
    print(f"Clean rows: {cleaned.count()}")

    print("\nRevenue by category:")
    by_category.show()
    print("Revenue by city:")
    by_city.show()
    print("Monthly revenue:")
    monthly.show(12)

    load(cleaned, "output/cleaned_sales")
    load(monthly, "output/monthly_revenue")
    print("Saved Parquet files to ./output")
    spark.stop()


if __name__ == "__main__":
    main()
