# main.py

import argparse
import logging
from src.data_loader import load_pharmacies, load_claims, load_reverts
from src.metrics_calculator import calculate_core_metrics
from src.analytics_engine import run_advanced_analytics


# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline(pharmacies_path, claims_path, reverts_path):

    """Main entry point for the data processing application."""

    logging.info("Starting data pipeline...")

    # --- GOAL 1: Read data ---
    try:
        pharmacies_df = load_pharmacies(pharmacies_path)
        if pharmacies_df.empty:
            logging.error("Pharmacy data is essential. Exiting.")
            return

        valid_npis = set(pharmacies_df['npi'].astype(str).unique())
        
        claims_df = load_claims(claims_path, valid_npis=valid_npis)
        reverts_df = load_reverts(reverts_path)
        
    except Exception as e:
        logging.error(f"A critical error occurred during data loading: {e}", exc_info=True)
        return

    if claims_df.empty:
        logging.warning("No valid claims found to process. Exiting.")
        return

    # --- GOAL 2: Calculate core metrics ---
    calculate_core_metrics(claims_df, reverts_df) # <-- Call the new function

    # --- GOALS 3 & 4 (Placeholder) ---

    run_advanced_analytics(claims_df, pharmacies_df) # <-- Call the Spark function
    
    logging.info("Pipeline finished successfully.")
    logging.info("Core metrics calculated. Ready for advanced analytics.")
    

def main():
    """
    Función de entrada solo para la línea de comandos.
    Su única responsabilidad es parsear los argumentos y llamar al pipeline.
    """
    parser = argparse.ArgumentParser(description="Process pharmacy claims and reverts data.")
    parser.add_argument("--pharmacies", required=True, help="Directory containing pharmacy CSV files.")
    parser.add_argument("--claims", required=True, help="Directory containing claims JSON files.")
    parser.add_argument("--reverts", required=True, help="Directory containing reverts JSON files.")
    args = parser.parse_args()

    # Llama a la lógica principal con los argumentos recibidos
    run_pipeline(pharmacies_path=args.pharmacies, 
                 claims_path=args.claims, 
                 reverts_path=args.reverts)

if __name__ == "__main__":
    # Este bloque solo se ejecuta cuando corres el archivo desde la terminal
    main()