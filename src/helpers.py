import os 
import logging
import pandas as pd

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