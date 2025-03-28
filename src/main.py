import os
from dotenv import load_dotenv
from data_loader import load_delitos_df, load_contact_df, load_renta_df

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
    # Join renta_df and contact_df on CP val; removing values that are not contained in both 
    # ASSUMPTION - Only purpose of analysis is to assess how house types and income levels affect crime rates
    contact_df['CP'] = contact_df['CP'].astype(str).str.strip()
    renta_df['CP'] = renta_df['CP'].astype(str).str.strip()

    merged_df = pd.merge(contact_df, renta_df, on="CP", how="inner")

    # Join merged_df with municipio data to only consider municipios with crime data
    # Ensure both DataFrames have a standardized "Municipio" column (e.g., trimmed strings)
    merged_df['Municipio'] = merged_df['Municipios'].astype(str).str.strip()
    delitos_df['Municipio'] = delitos_df['Municipio'].astype(str).str.strip()

    # Perform an inner join on "Municipio"
final_df = pd.merge(merged_df, delitos_df, on="Municipio", how="inner")

if __name__ == "__main__":
    main()