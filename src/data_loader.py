# src/data_loader.py
import pandas as pd
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Set
from concurrent.futures import ProcessPoolExecutor, as_completed
from . import config

def _read_and_validate_json_list(file_path: Path, required_keys: List[str]) -> List[Dict[str, Any]]:
    """
    Helper function to read a JSON file containing a list of objects,
    validating that each object contains the minimum required keys.
    """
    valid_records = []
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("JSON content is not a list of events.")
            
            for record in data:
                if isinstance(record, dict) and all(key in record for key in required_keys):
                    valid_records.append(record)
                else:
                    logging.warning(f"Skipping record with missing required keys in {file_path}: {record}")
        return valid_records
    except (json.JSONDecodeError, ValueError) as e:
        logging.warning(f"Skipping corrupt or invalid file {file_path}: {e}")
        return []

def load_pharmacies(pharmacy_dir: str) -> pd.DataFrame:
    """Loads and returns the pharmacy data as a DataFrame."""
    logging.info(f"Loading pharmacies from {pharmacy_dir}...")
    try:
        pharmacy_path = next(Path(pharmacy_dir).glob('*.csv'))
        pharmacies_df = pd.read_csv(pharmacy_path, dtype={'npi': str})
        logging.info(f"Loaded {len(pharmacies_df)} pharmacy records.")
        return pharmacies_df
    except StopIteration:
        logging.error(f"No CSV file found in pharmacy directory: {pharmacy_dir}")
        return pd.DataFrame()

def _load_event_data(directory: str, required_keys: List[str], valid_npis: Set[str] = None) -> pd.DataFrame:
    """Generic parallel loader for JSON event data (claims and reverts)."""
    files = list(Path(directory).glob('*.json'))
    if not files:
        return pd.DataFrame()

    all_records = []
    with ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(_read_and_validate_json_list, file, required_keys): file for file in files}
        for future in as_completed(future_to_file):
            records = future.result()
            if valid_npis:
                # Filter by NPI after initial validation
                filtered_records = [rec for rec in records if str(rec.get("npi")) in valid_npis]
                all_records.extend(filtered_records)
            else:
                all_records.extend(records)
    
    return pd.DataFrame(all_records)

def load_claims(claims_dir: str, valid_npis: Set[str]) -> pd.DataFrame:
    """Loads, validates, and filters claims data in parallel."""
    logging.info(f"Loading claims from {claims_dir}...")
    claims_df = _load_event_data(claims_dir, config.CLAIM_REQUIRED_KEYS, valid_npis)
    logging.info(f"Loaded {len(claims_df)} valid claims from known pharmacies.")
    return claims_df

def load_reverts(reverts_dir: str) -> pd.DataFrame:
    """Loads and validates reverts data in parallel."""
    logging.info(f"Loading reverts from {reverts_dir}...")
    reverts_df = _load_event_data(reverts_dir, config.REVERT_REQUIRED_KEYS)
    logging.info(f"Loaded {len(reverts_df)} reverts.")
    return reverts_df