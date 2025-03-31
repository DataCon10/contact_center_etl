import os
import logging
from dotenv import load_dotenv

from data_loader import load_delitos_df, load_contact_df, load_renta_df
from helpers import load_env_paths, merge_renta_delitos, merge_with_contact

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)



def main():
    load_dotenv()

    # Load file paths from environment
    delitos_file, contact_file, renta_file = load_env_paths()
    logger.info("Loaded environment paths.")

    # Load data
    delitos_df = load_delitos_df(delitos_file)
    contact_df = load_contact_df(contact_file)
    renta_df   = load_renta_df(renta_file)
    logger.info("Data successfully loaded.")

    # Merge renta and delitos datasets
    merged_renta_delitos = merge_renta_delitos(renta_df, delitos_df)

    # Final merge with contact dataset
    final_df = merge_with_contact(merged_renta_delitos, contact_df)

    # (Optional) Save final DataFrame or further process here
    # final_df.to_csv('data/final_dataset.csv', index=False)
    logger.info("Data merging complete.")

if __name__ == "__main__":
    main()
