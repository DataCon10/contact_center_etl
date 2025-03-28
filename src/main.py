import pandas as pd
import numpy as np
import csv

# 1.a LOAD DATA
# -----------------------------------------------------------------------------
# Delitos 
# Assummption - Headers are not revelant for dataset

import pandas as pd
import numpy as np

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

    # Convert MultiIndex columns to a DataFrame for easier manipulation
    cols_df = df.columns.to_frame(index=False)
    # Replace any top-level header starting with "Unnamed" with NaN and forward-fill them
    cols_df.iloc[:, 0] = cols_df.iloc[:, 0].replace(r"^Unnamed.*", np.nan, regex=True).ffill()
    
    # Rebuild the MultiIndex from the cleaned DataFrame
    df.columns = pd.MultiIndex.from_frame(cols_df)
    
    # Flatten the MultiIndex into a single string per column
    df.columns = [f"{str(upper).strip()}_{str(lower).strip()}" for upper, lower in df.columns]
    
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

    # Drop unwanted column if it exists
    if "TOTAL_INFRACCIONES_PENALES_31_level_1" in df.columns:
        df.drop(columns=["TOTAL_INFRACCIONES_PENALES_31_level_1"], inplace=True)
    
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


if __name__ == "__main__":
    delitos_df = load_delitos_df("data/delitos_por_municipio.csv")
    
    contact_df = load_contact_df("data/contac_center_data.csv")
    renta_df = load_renta_df("data/renta_por_hogar.csv", skiprows=0, skipfooter=0)  # adjust skiprows/skipfooter as needed


# 1b. LOAD DATA
# -----------------------------------------------------------------------------
# Contact

contact_df = pd.read_csv(r"data/contac_center_data.csv", sep=';')


# renta_df   = pd.read_csv(r"data/renta_por_hogar.csv", sep=';')




