import os
import logging
from dotenv import load_dotenv

# from data_loader import load_delitos_df, load_contact_df, load_renta_df
from data_loader import DataLoader
from helpers import load_env_paths, merge_renta_delitos, merge_with_contact

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)



def main():
    load_dotenv()
    
    # Create an instance of DataLoader
    loader = DataLoader()
    
    # Load datasets using the class methods (file paths come from environment variables if not provided)
    delitos_df = loader.load_delitos_df()
    contact_df = loader.load_contact_df()
    renta_df   = loader.load_renta_df()

    merged_renta_delitos = merge_renta_delitos(renta_df, delitos_df)

    # Final merge with contact dataset
    final_df = merge_with_contact(merged_renta_delitos, contact_df)

    # (Optional) Save final DataFrame or further process here
    # final_df.to_csv('data/final_dataset.csv', index=False)
    logger.info("Data merging complete.")



if __name__ == "__main__":
    main()
