# src/config.py

# Schemas for validation. Defines expected types for robust parsing.
PHARMACY_SCHEMA = {
    "id": str,
    "chain": str,
    "npi": str
}

CLAIM_SCHEMA = {
    "id": str,
    "npi": str,
    "ndc": str,
    "price": float,
    "quantity": (int, float), # Accept both int and float for quantity
    "timestamp": str
}

# Minimum required keys to consider a record valid.
CLAIM_REQUIRED_KEYS = ["id", "npi", "ndc", "price", "quantity", "timestamp"]

REVERT_SCHEMA = {
    "id": str,
    "claim_id": str,
    "timestamp": str
}

REVERT_REQUIRED_KEYS = ["id", "claim_id", "timestamp"]

# Output paths
METRICS_BY_DIMENSION_OUTPUT = "output/metrics_by_dimension.json"
TOP_CHAINS_OUTPUT = "output/top_2_chains_per_drug.json"
COMMON_QUANTITY_OUTPUT = "output/most_common_quantity_per_drug.json"