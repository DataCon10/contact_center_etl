import os
import pandas as pd
import numpy as np
import logging
from dotenv import load_dotenv

class DataLoader:
    """
    DataLoader encapsulates methods for loading and cleaning the delitos, contact, 
    and renta datasets. It centralizes configuration parameters (like file paths and encodings)
    and provides methods that return processed DataFrames.
    """
    
    def __init__(self,
                 delitos_file: str = None,
                 contact_file: str = None,
                 renta_file: str = None,
                 sep: str = ';',
                 encoding_delitos: str = 'latin1',
                 encoding_contact: str = 'utf-8',
                 encoding_renta: str = 'utf-8-sig',
                 delitos_skiprows: int = 5,
                 delitos_skipfooter: int = 7,
                 renta_skiprows: int = 0,
                 renta_skipfooter: int = 0):
        # Load environment variables from .env if file paths are not provided
        load_dotenv()
        self.delitos_file = delitos_file or os.getenv("DELITOS_FILE")
        self.contact_file = contact_file or os.getenv("CONTACT_FILE")
        self.renta_file   = renta_file or os.getenv("RENTA_FILE")
        
        self.sep = sep
        self.encoding_delitos = encoding_delitos
        self.encoding_contact = encoding_contact
        self.encoding_renta = encoding_renta
        
        self.delitos_skiprows = delitos_skiprows
        self.delitos_skipfooter = delitos_skipfooter
        self.renta_skiprows = renta_skiprows
        self.renta_skipfooter = renta_skipfooter
        
        # Set up the logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
        self.logger.info("DataLoader initialized with default settings.")

    def load_delitos_df(self, file_path: str = None) -> pd.DataFrame:
        """
        Load and clean the 'delitos_por_municipio' CSV file.
        
        Reads the CSV with multi-level headers, cleans and flattens the header,
        renames and formats columns, and drops unwanted artifacts.
        
        Parameters:
            file_path (str, optional): Path to the delitos CSV. If None, uses the default.
        
        Returns:
            pd.DataFrame: Cleaned DataFrame with descriptive column names.
        """
        file_path = file_path or self.delitos_file
        self.logger.info(f"Loading delitos data from {file_path}")
        df = pd.read_csv(
            file_path,
            sep=self.sep,
            header=[0, 1],
            encoding=self.encoding_delitos,
            engine='python',  # Required when using skipfooter
            skiprows=self.delitos_skiprows,
            skipfooter=self.delitos_skipfooter
        )
        self.logger.info(f"Delitos data loaded with shape: {df.shape}")

        # Convert multi-level columns to a DataFrame for manipulation
        cols_df = df.columns.to_frame(index=False)
        self.logger.debug(f"Initial header DataFrame:\n{cols_df}")

        # Replace any 'Unnamed' top-level header with NaN and forward-fill
        cols_df.iloc[:, 0] = (
            cols_df.iloc[:, 0]
            .replace(r"^Unnamed.*", np.nan, regex=True)
            .ffill()
        )
        self.logger.debug(f"Header DataFrame after forward-fill:\n{cols_df}")

        # Rebuild the MultiIndex and flatten it
        df.columns = pd.MultiIndex.from_frame(cols_df)
        df.columns = [f"{str(upper).strip()}_{str(lower).strip()}" 
                      for upper, lower in df.columns]
        self.logger.info(f"Flattened column names: {df.columns.tolist()}")

        # Rename first column to "Municipio" and clean its values
        df.rename(columns={df.columns[0]: "Municipio"}, inplace=True)
        df["Municipio"] = df["Municipio"].str.replace(r"^- Municipio de\s*", "", regex=True)

        # Refine column names to be more descriptive
        new_cols = {}
        for col in df.columns:
            parts = col.split("_", maxsplit=1)
            if len(parts) == 2:
                crime_type, period = parts
                crime_type_clean = crime_type.split('.', 1)[-1].strip("- .")
                year = period.split()[-1]  # Extract year from period
                new_cols[col] = f"{crime_type_clean.replace(' ', '_')}_{year}"
            else:
                new_cols[col] = col
        df.rename(columns=new_cols, inplace=True)
        self.logger.info(f"Renamed columns: {df.columns.tolist()}")

        # Drop unwanted column if it exists
        unwanted_col = "TOTAL_INFRACCIONES_PENALES_31_level_1"
        if unwanted_col in df.columns:
            df.drop(columns=[unwanted_col], inplace=True)
            self.logger.info(f"Dropped unwanted column: {unwanted_col}")

        self.logger.info(f"Final delitos DataFrame shape: {df.shape}")
        return df

    def load_contact_df(self, file_path: str = None) -> pd.DataFrame:
        """
        Load and clean the contact center CSV file, then pivot funnel_Q responses aggregated by CP.
        
        Steps:
          1. Loads the CSV.
          2. Drops columns that are entirely NaN.
          3. Cleans the 'sessionID' column by removing unwanted characters.
          4. Drops duplicate rows (keeping one unique answer per DNI, CP, and funnel_Q).
          5. Counts unique DNIs per CP.
          6. Pivots the DataFrame so that each funnel_Q response becomes its own column with count of unique DNIs.
          7. Merges the DNI totals with the pivoted counts.
        
        Parameters:
            file_path (str, optional): Path to the contact CSV. If None, uses the default.
        
        Returns:
            pd.DataFrame: Aggregated DataFrame with one row per CP.
        """
        file_path = file_path or self.contact_file
        self.logger.info(f"Loading contact data from {file_path}")
        df = pd.read_csv(file_path, sep=self.sep, encoding=self.encoding_contact)
        df.dropna(axis=1, how='all', inplace=True)
        
        # Clean the 'sessionID' column
        df['sessionID'] = (
            df['sessionID']
            .str.replace(r"^b['\"]", "", regex=True)
            .str.replace(r"['\"]$", "", regex=True)
            .str.replace(r"=+$", "", regex=True)
        )
        
        # Drop duplicates based on DNI, CP, and funnel_Q
        final_contact_df = df.drop_duplicates(subset=['DNI', 'CP', 'funnel_Q'])
        
        # Count total unique DNIs per CP
        dni_totals = final_contact_df.groupby('CP')['DNI'].nunique().reset_index(name='total_DNIs')
        
        # Pivot the funnel_Q responses so each response becomes its own column (with unique DNI counts)
        pivot_df = final_contact_df.pivot_table(
            index='CP',
            columns='funnel_Q',
            values='DNI',
            aggfunc='nunique',
            fill_value=0
        ).reset_index()
        
        # Merge the total DNI counts with the pivoted responses on CP
        merged_df = pd.merge(dni_totals, pivot_df, on='CP', how='left')
        return merged_df

    def load_renta_df(self, file_path: str = None) -> pd.DataFrame:
        """
        Load and process the renta CSV file.

        The function performs the following steps:
          1. Reads the CSV with given parameters.
          2. Filters the data to include only records for periods 2019 and 2020.
          3. Splits the 'Municipios' column into a postal code ('CP') and municipality name.
          4. Extracts district and section codes from 'Distritos' and 'Secciones'.
          5. Cleans the 'Total' column by replacing dot-only values with NaN and converting it to numeric.
          6. Imputes missing 'Total' values within each (Municipios, Periodo) group using the group's median.
             Any remaining NaNs are filled with the overall median.
          7. Groups the data by Municipios, CP, and Periodo, taking the median of the imputed totals.
        
        Parameters:
            file_path (str, optional): Path to the renta CSV file. If None, uses the default.
        
        Returns:
            pd.DataFrame: A grouped DataFrame with columns [Municipios, CP, Periodo, total_imputed],
                          where total_imputed is the median imputed Total.
        """
        file_path = file_path or self.renta_file
        self.logger.info(f"Loading renta data from {file_path}")
        df = pd.read_csv(
            file_path,
            sep=self.sep,
            encoding=self.encoding_renta,
            engine='python',  
            skiprows=self.renta_skiprows,
            skipfooter=self.renta_skipfooter
        )
        
        # Filter data to include only records for periods 2019 and 2020
        df = df[df['Periodo'].isin([2019, 2020])]
        
        # Split 'Municipios' into 'CP' and updated 'Municipios'
        df[['CP', 'Municipios']] = df['Municipios'].str.extract(r"^(\d{5})\s+(.*)")
        
        # Extract district and section codes
        df['Distritos'] = df['Distritos'].str.extract(r"distrito\s+(\d+)$")
        df['Secciones'] = df['Secciones'].str.extract(r"sección\s+(\d+)$")
        
        # Clean the 'Total' column: replace dot-only values with NaN and convert to numeric
        df['Total'] = df['Total'].replace(r'^\.+$', np.nan, regex=True)
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce')
        
        # Impute missing 'Total' values within each (Municipios, Periodo) group using the group's median
        df['total_imputed'] = df.groupby(['Municipios', 'Periodo'])['Total'] \
                                .transform(lambda x: x.fillna(x.median()))
        # Fill any remaining NaNs with the overall median
        df['total_imputed'] = df['total_imputed'].fillna(df['total_imputed'].median())
        
        # Group the data by Municipios, CP, and Periodo, taking the median of the imputed totals
        grouped_df = df.groupby(["Municipios", "CP", "Periodo"], as_index=False)["total_imputed"].median()
        return grouped_df

