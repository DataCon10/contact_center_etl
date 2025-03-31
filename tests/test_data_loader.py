import pytest
import pandas as pd
from io import StringIO
import os
import sys 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data_loader import DataLoader

def test_dataloader_initialization():
    # Create a DataLoader instance with test file paths.
    loader = DataLoader(
        delitos_file="dummy_delitos.csv",
        contact_file="dummy_contact.csv",
        renta_file="dummy_renta.csv"
    )
    # Check that the file path attributes are correctly set.
    assert loader.delitos_file == "dummy_delitos.csv"
    assert loader.contact_file == "dummy_contact.csv"
    assert loader.renta_file == "dummy_renta.csv"

def test_load_delitos_df_minimal():
    # Create a dummy CSV content for delitos.
    dummy_csv = StringIO(
        "Skip line 1\n"
        "Skip line 2\n"
        "Skip line 3\n"
        "Skip line 4\n"
        "Skip line 5\n"
        "1.-Test;Unnamed: 1\n"
        "Enero-marzo 2019;Enero-marzo 2020\n"
        "Test Municipio;10\n"
    )
    
    # Initialize DataLoader (file paths will be overridden in the test)
    loader = DataLoader()
    
    # Call load_delitos_df with dummy CSV input
    df = loader.load_delitos_df(file_path=dummy_csv)
    
    # Verify that the DataFrame contains the 'Municipio' column after processing.
    assert "Municipio" in df.columns, "Expected 'Municipio' column to be present."
    


