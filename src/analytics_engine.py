

import json
import pandas as pd
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, row_number, collect_list, struct, asc
from . import config

def run_advanced_analytics(claims_df: pd.DataFrame, pharmacies_df: pd.DataFrame):
    """
    Runs advanced analytics using PySpark to calculate KPIs for Goals 3 and 4.
    """
    logging.info("Starting advanced analytics with Spark...")

    # 1. Initialize a local Spark Session using all available cores
    spark = (
        SparkSession.builder.appName("HealthTechAnalytics")
        .master("local[*]") 
        .config("spark.driver.memory", "4g") 
        .getOrCreate()
    )

    # Convert Pandas DataFrames to Spark DataFrames
    claims_spark_df = spark.createDataFrame(claims_df)
    pharmacies_spark_df = spark.createDataFrame(pharmacies_df)

    # Filter for valid, non-reverted claims
    valid_claims_df = claims_spark_df.filter(col("is_reverted") == False)
    
    # Calculate unit price, handling zero quantity
    claims_with_unit_price = valid_claims_df.withColumn(
        "unit_price",
        col("price") / (col("quantity") + 0.0001)
    )

    # --- Goal 3: Top 2 Chains per Drug ---
    calculate_top_chains(claims_with_unit_price, pharmacies_spark_df)

    # --- Goal 4: Most Common Quantity per Drug ---
    calculate_most_common_quantity(valid_claims_df)
    
    spark.stop()
    logging.info("Advanced analytics finished.")


def calculate_top_chains(claims_df, pharmacies_df):
    """Calculates and saves the top 2 chains with the lowest average unit price per drug."""
    
    # Join claims with pharmacies to get the chain information
    enriched_claims = claims_df.join(pharmacies_df, on="npi", how="inner")

    # Calculate the average unit price per drug (ndc) and chain
    avg_price_by_chain = enriched_claims.groupBy("ndc", "chain").agg(
        avg("unit_price").alias("avg_price")
    )

    # Use a window function to rank chains within each drug category
    window_spec = Window.partitionBy("ndc").orderBy(col("avg_price").asc())
    ranked_chains = avg_price_by_chain.withColumn("rank", row_number().over(window_spec))

    # Filter for the top 2 ranks
    top_2_chains = ranked_chains.filter(col("rank") <= 2)

    # Format the output to match the required JSON structure
    formatted_output = (
        top_2_chains.groupBy("ndc")
        .agg(
            collect_list(
                struct(col("chain").alias("name"), col("avg_price"))
            ).alias("chain")
        )
    )

    # Collect the results and write to a JSON file
    top_chains_result = formatted_output.toJSON().map(lambda j: json.loads(j)).collect()
    
    output_path = config.TOP_CHAINS_OUTPUT
    with open(output_path, 'w') as f:
        json.dump(top_chains_result, f, indent=4)
    logging.info(f"Successfully wrote top 2 chains data to {output_path}")


def calculate_most_common_quantity(claims_df):
    """Calculates and saves the most common prescription quantity per drug."""

    # Count the frequency of each quantity for each drug
    quantity_counts = claims_df.groupBy("ndc", "quantity").count()

    # Use a window function to find the most frequent quantity for each drug
    window_spec = Window.partitionBy("ndc").orderBy(col("count").desc())
    ranked_quantities = quantity_counts.withColumn("rank", row_number().over(window_spec))

    most_common_quantities = ranked_quantities.filter(col("rank") <= 5)

    most_common_quantities = most_common_quantities.orderBy(asc("ndc"),asc("rank"))

    # Format the output to get a list of quantities for each drug
    formatted_output = (
        most_common_quantities.groupBy("ndc")
        .agg(collect_list("quantity").alias("most_prescribed_quantity"))
    )

    # Collect the results and write to a JSON file
    common_quantity_result = formatted_output.toJSON().map(lambda j: json.loads(j)).collect()

    output_path = config.COMMON_QUANTITY_OUTPUT
    with open(output_path, 'w') as f:
        json.dump(common_quantity_result, f, indent=4)
    logging.info(f"Successfully wrote most common quantity data to {output_path}")