import pandas as pd
import numpy as np

import logging

# Set up a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Optionally add a StreamHandler if not configured globally
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def load_delitos_df(file_path: str, skiprows: int = 5, skipfooter: int = 7, sep: str = ';', encoding: str = 'latin1') -> pd.DataFrame:
    """
    Load and clean the 'delitos_por_municipio' CSV file.

    Parameters:
        file_path (str): Path to the CSV file.
        skiprows (int): Number of rows to skip at the beginning of the file.
        skipfooter (int): Number of rows to skip at the end of the file.
        sep (str): CSV delimiter.
        encoding (str): File encoding.
    
    Returns:
        pd.DataFrame: Cleaned DataFrame with flattened and descriptive column names.
    """
    logger.info(f"Loading delitos data from {file_path}")
    # Load the CSV with two header rows (after skipping initial descriptive rows)
    df = pd.read_csv(
        file_path,
        sep=sep,
        header=[0, 1],
        encoding=encoding,
        engine='python',  # Required when using skipfooter
        skiprows=skiprows,
        skipfooter=skipfooter
    )

    logger.info("Loaded delitos data with shape %s", df.shape)

    # Convert MultiIndex columns to a DataFrame for easier manipulation
    cols_df = df.columns.to_frame(index=False)
    logger.debug("Initial header DataFrame:\n%s", cols_df)
    # Replace any top-level header starting with "Unnamed" with NaN and forward-fill them
    cols_df.iloc[:, 0] = cols_df.iloc[:, 0].replace(r"^Unnamed.*", np.nan, regex=True).ffill()
    logger.debug("Header DataFrame after forward fill:\n%s", cols_df)

    
    # Rebuild the MultiIndex from the cleaned DataFrame
    df.columns = pd.MultiIndex.from_frame(cols_df)
    
    # Flatten the MultiIndex into a single string per column
    df.columns = [f"{str(upper).strip()}_{str(lower).strip()}" for upper, lower in df.columns]
    logger.info("Flattened columns: %s", df.columns.tolist())
    
    # Rename the first column to "Municipio"
    df.rename(columns={df.columns[0]: "Municipio"}, inplace=True)
    
    # Clean the Municipio values by removing the prefix "- Municipio de"
    df["Municipio"] = df["Municipio"].str.replace(r"^- Municipio de\s*", "", regex=True)
    
    # Further rename columns to be more descriptive:
    # e.g., "1.-Homicidios dolosos y asesinatos consumados_Enero-marzo 2019"
    # becomes "HomicidiosConsumados_2019"
    new_cols = {}
    for col in df.columns:
        parts = col.split("_", maxsplit=1)
        if len(parts) == 2:
            crime_type, period = parts
            # Remove leading digits, punctuation, and extra spaces from the crime type
            crime_type_clean = crime_type.split('.', 1)[-1].strip("- .")
            # Extract just the year from the period (assuming it's the last token)
            year = period.split()[-1]
            new_col = f"{crime_type_clean.replace(' ', '_')}_{year}"
            new_cols[col] = new_col
        else:
            new_cols[col] = col
    df.rename(columns=new_cols, inplace=True)

    logger.info("Renamed columns: %s", df.columns.tolist())

    # Drop unwanted column if it exists
    if "TOTAL_INFRACCIONES_PENALES_31_level_1" in df.columns:
        df.drop(columns=["TOTAL_INFRACCIONES_PENALES_31_level_1"], inplace=True)
    
    logger.info("Final delitos DataFrame shape: %s", df.shape)

    return df


def load_contact_df(file_path: str, sep: str = ';', encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Load the contact center CSV file.
    
    Adjust parameters if you need to skip any descriptive rows.
    
    Parameters:
        file_path (str): Path to the contact CSV.
        sep (str): Delimiter used in the CSV.
        encoding (str): File encoding.
    
    Returns:
        pd.DataFrame: The contact center DataFrame.
    """
    df = pd.read_csv(file_path, sep=sep, encoding=encoding)
    # Drop unnecessary NaN columns - assumption is that all NaN columns do not provide value 
    df = df.dropna(axis=1, how='all')

    # Clean the 'sessionID' column 
    df['sessionID'] = (
        df['sessionID']
        .str.replace(r"^b['\"]", "", regex=True)  # Remove leading b' or b"
        .str.replace(r"['\"]$", "", regex=True)    # Remove trailing ' or "
        .str.replace(r"=+$", "", regex=True)         # Remove one or more trailing '=' characters
    )

    return df


def load_renta_df(file_path: str, sep: str = ';', encoding: str = 'latin1', skiprows: int = 0, skipfooter: int = 0) -> pd.DataFrame:
    """
    Load the renta CSV file.
    
    The renta data typically has a simple header but may require skipping extra descriptive rows or footer rows.
    
    Parameters:
        file_path (str): Path to the renta CSV.
        sep (str): Delimiter used in the CSV.
        encoding (str): File encoding.
        skiprows (int): Number of rows to skip at the beginning.
        skipfooter (int): Number of rows to skip at the end.
    
    Returns:
        pd.DataFrame: The renta DataFrame.
    """
    df = pd.read_csv(file_path, sep=sep, encoding=encoding, engine='python', skiprows=skiprows, skipfooter=skipfooter)

    return df