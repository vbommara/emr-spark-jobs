#!/usr/bin/env python3
"""
Customer Analytics ETL - Production Version 1.0
Processes daily customer transaction data and generates insights
Author: Data Engineering Team
Last Updated: 2026-02-03
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, max as _max, min as _min
import sys

def main():
    if len(sys.argv) < 3:
        print("Usage: customer_analytics.py <input_path> <output_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    spark = SparkSession.builder \
        .appName("CustomerAnalytics-v1.0") \
        .getOrCreate()
    
    print("=" * 60)
    print("Customer Analytics ETL - Version 1.0")
    print("=" * 60)
    
    # Read customer transaction data
    print(f"Reading data from: {input_path}")
    transactions = spark.read.csv(input_path, header=True, inferSchema=True)
    
    print(f"Total transactions loaded: {transactions.count()}")
    transactions.printSchema()
    
    # Calculate customer metrics
    print("\nCalculating customer metrics...")
    customer_metrics = transactions.groupBy("customer_id").agg(
        _sum("amount").alias("total_spent"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value"),
        _max("amount").alias("max_order"),
        _min("amount").alias("min_order")
    )
    
    # Calculate customer segments
    print("Segmenting customers...")
    from pyspark.sql.functions import when
    
    segmented_customers = customer_metrics.withColumn(
        "customer_segment",
        when(col("total_spent") >= 1000, "Premium")
        .when(col("total_spent") >= 500, "Gold")
        .when(col("total_spent") >= 100, "Silver")
        .otherwise("Bronze")
    )
    
    print("\nCustomer Segments Summary:")
    segmented_customers.groupBy("customer_segment").count().show()
    
    print("\nTop 10 Customers by Total Spend:")
    segmented_customers.orderBy(col("total_spent").desc()).show(10)
    
    # Write results
    print(f"\nWriting results to: {output_path}")
    segmented_customers.coalesce(1).write.mode("overwrite").csv(
        output_path, header=True
    )
    
    print("=" * 60)
    print("✓ Customer Analytics completed successfully!")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()
