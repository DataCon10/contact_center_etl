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


if __name__ == "__main__":
    main()