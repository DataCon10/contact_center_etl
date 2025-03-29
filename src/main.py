import os
import logging
from dotenv import load_dotenv
import pandas as pd

from data_loader import load_delitos_df, load_contact_df, load_renta_df

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def load_env_paths():
    """Load file paths from environment variables."""
    delitos_file = os.getenv("DELITOS_FILE")
    contact_file = os.getenv("CONTACT_FILE")
    renta_file = os.getenv("RENTA_FILE")
    return delitos_file, contact_file, renta_file

def merge_renta_delitos(renta_df: pd.DataFrame, delitos_df: pd.DataFrame) -> pd.DataFrame:
    """Merge renta and delitos datasets on Municipio."""
    renta_df['Municipio'] = renta_df['Municipios'].astype(str).str.strip()
    delitos_df['Municipio'] = delitos_df['Municipio'].astype(str).str.strip()
    
    merged_df = pd.merge(renta_df, delitos_df, on="Municipio", how="inner")
    logger.info(f"Merged renta and delitos datasets; shape={merged_df.shape}")
    
    return merged_df

def merge_with_contact(merged_df: pd.DataFrame, contact_df: pd.DataFrame) -> pd.DataFrame:
    """Left join the merged renta-delitos data with contact data on CP."""
    merged_df['CP'] = merged_df['CP'].astype(str).str.strip()
    contact_df['CP'] = contact_df['CP'].astype(str).str.strip()

    final_df = pd.merge(merged_df, contact_df, on="CP", how="left")
    logger.info(f"Joined with contact data; final shape={final_df.shape}")
    
    return final_df

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
