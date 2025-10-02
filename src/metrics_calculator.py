# src/metrics_calculator.py

import pandas as pd
import logging
from . import config

def calculate_core_metrics(claims_df: pd.DataFrame, reverts_df: pd.DataFrame):
    """
    Calculates core business metrics from claims and reverts data.
    - Handles reverted claims.
    - Calculates unit price.
    - Aggregates metrics by NPI and NDC.
    - Saves the result to a JSON file.
    """
    if claims_df.empty:
        logging.warning("Claims DataFrame is empty. Cannot calculate metrics.")
        return

    try:
        # 1. Identify and handle reverted claims
        reverted_claim_ids = set(reverts_df['claim_id'])

        # Add a boolean flag for reverted status
        claims_df['is_reverted'] = claims_df['id'].isin(reverted_claim_ids)

        # Filter for valid, non-reverted claims for price calculations
        valid_claims_df = claims_df[~claims_df['is_reverted']].copy()

        # 2. Calculate unit price, handling potential division by zero
        # Replace 0 or negative quantities with NaN to avoid errors and exclude from avg calculation
        valid_claims_df['quantity'] = valid_claims_df['quantity'].mask(valid_claims_df['quantity'] <= 0)
        valid_claims_df['unit_price'] = valid_claims_df['price'] / valid_claims_df['quantity']

        # 3. Aggregate metrics
        # Group by npi and ndc from the original claims_df to include all claims (reverted or not)
        agg_metrics = claims_df.groupby(['npi', 'ndc']).agg(
            fills=('id', 'count'),
            reverted=('is_reverted', 'sum')
        )

        # Group by npi and ndc from the valid_claims_df for price calculations
        price_metrics = valid_claims_df.groupby(['npi', 'ndc']).agg(
            avg_price=('unit_price', 'mean'),
            total_price=('price', 'sum')
        )

        # 4. Join the aggregated results
        final_df = agg_metrics.join(price_metrics, on=['npi', 'ndc']).reset_index()

        # Fill NaN values that result from joins with 0
        final_df.fillna(0, inplace=True)

        # Format the final DataFrame to match the required output
        final_df['reverted'] = final_df['reverted'].astype(int)
        final_df['avg_price'] = final_df['avg_price'].round(2)
        final_df['total_price'] = final_df['total_price'].round(2)

        logging.info(f"Calculated metrics for {len(final_df)} NPI/NDC combinations.")

        # 5. Write output to JSON file
        output_path = config.METRICS_BY_DIMENSION_OUTPUT
        final_df.to_json(output_path, orient='records', indent=4)
        logging.info(f"Successfully wrote metrics to {output_path}")

        return final_df
    
    except Exception as e:
        logging.error(f"An unexpected error occurred during metric calculation: {e}", exc_info=True)
        return None