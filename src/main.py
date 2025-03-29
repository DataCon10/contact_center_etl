import os
from dotenv import load_dotenv
from data_loader import load_delitos_df, load_contact_df, load_renta_df
import pandas as pd

# Load the environment variables from the .env file
load_dotenv()

def main():
     # Read file paths from environment variables (.env files)
    delitos_file = os.getenv("DELITOS_FILE")
    contact_file = os.getenv("CONTACT_FILE")
    renta_file = os.getenv("RENTA_FILE")

    # Load datasets using the functions defined in data_loader.py
    delitos_df = load_delitos_df(delitos_file)
    contact_df = load_contact_df(contact_file)
    renta_df   = load_renta_df(renta_file) 

    # TODO - Add in functions
    # Join renta and delitos on Municipio val; removing values that are not contained in both 
    # ASSUMPTION - Only purpose of analysis is to assess how house types and income levels affect crime rates
    delitos_df['Municipio'] = delitos_df['Municipio'].astype(str).str.strip()
    renta_df['Municipio'] = renta_df['Municipios'].astype(str).str.strip()

    merged_df = pd.merge(renta_df, delitos_df, on="Municipio", how="inner")

    merged_df['CP'] = merged_df['CP'].astype(str).str.strip()
    contact_df['CP'] = contact_df['CP'].astype(str).str.strip()

    final_df = pd.merge(merged_df, contact_df, on="CP", how="left")



if __name__ == "__main__":
    main()