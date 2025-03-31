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


def load_delitos_df(
    file_path: str,
    skiprows: int = 5,
    skipfooter: int = 7,
    sep: str = ';',
    encoding: str = 'latin1'
) -> pd.DataFrame:
    """
    Load and clean the 'delitos_por_municipio' CSV file.

    This function processes the input file, cleans multi-level column headers,
    flattens them, and formats the dataset for ease of use.

    Parameters:
        file_path (str): 
            Path to the delitos CSV file.
        skiprows (int, optional): 
            Number of rows to skip at the beginning of the file. Defaults to 5.
        skipfooter (int, optional): 
            Number of rows to skip at the end of the file. Defaults to 7.
        sep (str, optional): 
            CSV delimiter. Defaults to ';'.
        encoding (str, optional): 
            File encoding. Defaults to 'latin1'.

    Returns:
        pd.DataFrame: 
            Cleaned DataFrame with flattened and descriptive column names.
    """

    logger.info(f"Loading delitos data from {file_path}")

    # Load the CSV with multi-level headers, skipping unnecessary rows and footers
    df = pd.read_csv(
        file_path,
        sep=sep,
        header=[0, 1],
        encoding=encoding,
        engine='python',  # Required when using skipfooter
        skiprows=skiprows,
        skipfooter=skipfooter
    )
    logger.info(f"Delitos data loaded with shape: {df.shape}")

    # Convert multi-level column headers into a DataFrame for easier manipulation
    cols_df = df.columns.to_frame(index=False)
    logger.debug(f"Initial header DataFrame:\n{cols_df}")

    # Replace 'Unnamed' top-level headers with NaN, then forward-fill missing values
    cols_df.iloc[:, 0] = (
        cols_df.iloc[:, 0]
        .replace(r"^Unnamed.*", np.nan, regex=True)
        .ffill()
    )
    logger.debug(f"Header DataFrame after forward-fill:\n{cols_df}")

    # Rebuild the MultiIndex from the cleaned columns DataFrame
    df.columns = pd.MultiIndex.from_frame(cols_df)

    # Flatten the MultiIndex into a single-level header by concatenating levels
    df.columns = [
        f"{str(upper).strip()}_{str(lower).strip()}" 
        for upper, lower in df.columns
    ]
    logger.info(f"Flattened column names: {df.columns.tolist()}")

    # Rename the first column explicitly to 'Municipio'
    df.rename(columns={df.columns[0]: "Municipio"}, inplace=True)

    # Clean Municipio column values (remove '- Municipio de' prefix)
    df["Municipio"] = df["Municipio"].str.replace(r"^- Municipio de\s*", "", regex=True)

    # Create more readable and standardized column names
    new_cols = {}
    for col in df.columns:
        parts = col.split("_", maxsplit=1)
        if len(parts) == 2:
            crime_type, period = parts
            # Clean the crime type name by removing digits and punctuation
            crime_type_clean = crime_type.split('.', 1)[-1].strip("- .")
            # Extract the year from the period
            year = period.split()[-1]
            # Reformat the column name in a cleaner way
            new_col = f"{crime_type_clean.replace(' ', '_')}_{year}"
            new_cols[col] = new_col
        else:
            # For columns like "Municipio"
            new_cols[col] = col

    # Rename columns with new, cleaner names
    df.rename(columns=new_cols, inplace=True)
    logger.info(f"Renamed column names: {df.columns.tolist()}")

    # Remove unnecessary columns if present
    unwanted_col = "TOTAL_INFRACCIONES_PENALES_31_level_1"
    if unwanted_col in df.columns:
        df.drop(columns=[unwanted_col], inplace=True)
        logger.info(f"Dropped unwanted column: {unwanted_col}")

    logger.info(f"Final delitos DataFrame shape: {df.shape}")

    return df



def load_contact_df(file_path: str, sep: str = ';', encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Load and clean the contact center CSV file, then pivot funnel_Q responses aggregated by CP.

    This function performs the following steps:
      1. Loads the contact center CSV file.
      2. Drops columns that contain only NaN values.
      3. Cleans the 'sessionID' column by removing:
         - A leading "b'" or 'b"'
         - A trailing single or double quote
         - One or more trailing '=' characters.
      4. Drops duplicate rows so that only one unique answer per combination of DNI, CP, and funnel_Q is kept.
      5. Computes the total number of unique DNIs per CP.
      6. Pivots the DataFrame so that each funnel_Q response becomes its own column, 
         counting the number of unique DNIs per response.
      7. Merges the total DNI counts with the pivoted responses.

    Parameters:
        file_path (str): Path to the contact CSV file.
        sep (str): Delimiter used in the CSV file (default: ';').
        encoding (str): File encoding (default: 'utf-8').

    Returns:
        pd.DataFrame: A DataFrame with one row per CP, including:
            - The total number of distinct DNIs ('total_DNIs')
            - Count columns for each funnel_Q response.
    """
    # Load the contact center CSV file
    df = pd.read_csv(file_path, sep=sep, encoding=encoding)
    
    # Drop any columns that are completely NaN
    df.dropna(axis=1, how='all', inplace=True)
    
    # Clean the 'sessionID' column:
    # - Remove leading b' or b"
    # - Remove trailing single or double quotes
    # - Remove trailing '=' characters
    df['sessionID'] = (
        df['sessionID']
        .str.replace(r"^b['\"]", "", regex=True)
        .str.replace(r"['\"]$", "", regex=True)
        .str.replace(r"=+$", "", regex=True)
    )
    
    # Drop duplicate rows so that each combination of DNI, CP, and funnel_Q appears only once
    final_contact_df = df.drop_duplicates(subset=['DNI', 'CP', 'funnel_Q'])
    
    # Count the total number of unique DNIs per CP
    dni_totals = final_contact_df.groupby('CP')['DNI'].nunique().reset_index(name='total_DNIs')
    
    # Pivot the DataFrame so that each funnel_Q response becomes its own column.
    # The values are the number of unique DNIs giving that response.
    pivot_df = final_contact_df.pivot_table(
        index='CP',
        columns='funnel_Q',
        values='DNI',
        aggfunc='nunique',
        fill_value=0
    ).reset_index()
    
    # Merge the total DNIs with the pivoted response counts on CP
    merged_df = pd.merge(dni_totals, pivot_df, on='CP', how='left')
    
    return merged_df


# def validate_repeated_DNI_sessions(df: pd.DataFrame) -> pd.DataFrame:
    # pass

def load_renta_df(
    file_path: str,
    sep: str = ';',
    encoding: str = 'utf-8-sig',
    skiprows: int = 0,
    skipfooter: int = 0
) -> pd.DataFrame:
    """
    Load and process the renta CSV file.

    This function performs the following steps:
      1. Reads the CSV file using the specified parameters.
      2. Filters the data to include only records for the periods 2019 and 2020.
      3. Splits the 'Municipios' column into a postal code ('CP') and a municipality name.
      4. Extracts district and section codes from the 'Distritos' and 'Secciones' columns.
      5. Cleans the 'Total' column by converting dot-only values to NaN and then to numeric.
      6. Imputes missing 'Total' values within each (Municipios, Periodo) group using the group's median.
         If any missing values remain, they are filled with the overall median.
      7. Groups the data by Municipio, CP, and Periodo, taking the median of the imputed totals.

    Parameters:
        file_path (str): Path to the renta CSV file.
        sep (str): Delimiter used in the CSV. Default is ';'.
        encoding (str): File encoding. Default is 'utf-8-sig' to handle BOM if present.
        skiprows (int): Number of rows to skip at the beginning of the file.
        skipfooter (int): Number of rows to skip at the end of the file.

    Returns:
        pd.DataFrame: A grouped DataFrame with columns [Municipios, CP, Periodo, total_imputed],
                      where total_imputed is the median of the imputed 'Total' values.
    """
    # Load the CSV file with the specified parameters
    df = pd.read_csv(
        file_path,
        sep=sep,
        encoding=encoding,
        engine='python',  # Required when using skipfooter
        skiprows=skiprows,
        skipfooter=skipfooter
    )

    # Filter data to only include records for periods 2019 and 2020
    df = df[df['Periodo'].isin([2019, 2020])]

    # Split the 'Municipios' column into 'CP' (postal code) and updated 'Municipios'
    # Assumes the 'Municipios' column starts with a 5-digit postal code followed by a space and then the name.
    df[['CP', 'Municipios']] = df['Municipios'].str.extract(r"^(\d{5})\s+(.*)")

    # Extract district code: digits at the end of the 'Distritos' string after the word "distrito"
    df['Distritos'] = df['Distritos'].str.extract(r"distrito\s+(\d+)$")

    # Extract section code: digits at the end of the 'Secciones' string after the word "sección"
    df['Secciones'] = df['Secciones'].str.extract(r"sección\s+(\d+)$")

    # Clean the 'Total' column:
    # - Replace any value that consists solely of dots with NaN
    df['Total'] = df['Total'].replace(r'^\.+$', np.nan, regex=True)
    # - Convert 'Total' to numeric, coercing errors to NaN
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce')

    # Impute missing 'Total' values within each group (Municipios, Periodo) using the group's median
    df['total_imputed'] = df.groupby(['Municipios', 'Periodo'])['Total'] \
                            .transform(lambda x: x.fillna(x.median()))
    # For any remaining NaN values, fill with the overall median of total_imputed
    df['total_imputed'] = df['total_imputed'].fillna(df['total_imputed'].median())

    # Group by Municipio, CP, and Periodo, taking the median of the imputed totals
    grouped_df = df.groupby(["Municipios", "CP", "Periodo"], as_index=False)["total_imputed"].median()

    return grouped_df
