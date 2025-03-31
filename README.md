# Data Enrichment and Analysis Pipeline

This project builds a data engineering pipeline that loads, cleans, and merges three datasets from the Community of Madrid: crime data (delitos), contact center data, and income data (renta). The aim is to explore how property characteristics and income levels relate to crime rates.

## ETL Process Overview

1. **Delitos Data:**
   - **Load:** Skip the first 5 rows (descriptive) and last 7 rows (footers); read with two header rows.
   - **Clean:** Forward-fill missing top-level headers (e.g., "Unnamed") and flatten the MultiIndex.
   - **Transform:** Rename the first column to "Municipio" (removing prefixes) and standardize crime-type columns (e.g., "HomicidiosConsumados_2019").

2. **Contact Data:**
   - **Load:** Read the CSV and drop columns with all NaN values.
   - **Clean:** Remove extraneous characters from `sessionID` (e.g., leading `b'`, trailing quotes/equal signs).
   - **Deduplicate & Pivot:** Remove duplicate responses (by DNI, CP, funnel_Q) and pivot so each funnel_Q answer becomes its own column (with counts of unique DNIs per CP).

3. **Renta Data:**
   - **Load:** Read the CSV (using `utf-8-sig`), and filter for periods 2019 and 2020.
   - **Transform:** Split the "Municipios" column into CP (postal code) and municipality name; extract district and section codes.
   - **Clean & Impute:** Replace dot-only values in "Total" with NaN, convert to numeric, and impute missing values using the median per (Municipios, Periodo); aggregate by Municipio, CP, and Periodo.

4. **Merging:**
   - **Join 1:** Merge renta and delitos data on the standardized "Municipio" field (inner join).
   - **Join 2:** Left-join the merged data with contact data on CP.

## Assumptions and Data Issues

- **Metadata & Footers:**  
  The first 5 rows and the last 7 rows of the delitos CSV are assumed to be non-data (descriptive/footnotes).

- **Header Cleaning:**  
  Missing multi-level header values (labeled "Unnamed") are forward-filled.

- **Key Columns:**  
  The first column in the delitos data is assumed to represent the municipality; prefixes (e.g., "- Municipio de") are removed.  
  In the renta data, the "Municipios" field begins with a 5-digit CP.

- **Duplicates:**  
  Repeated contact sessions (same DNI, CP, and funnel_Q) are dropped to keep one answer per person per CP.

- **Temporal Focus:**  
  Only data for 2019 and 2020 are used, since crime data is available only for these periods.

- **Imputation:**  
  Missing income values (Total) are imputed using the median value within each (Municipios, Periodo) group, with any remaining missing values replaced by the overall median.


## Project includes:
- **Modular Code:** Functions and classes are separated into logical modules (e.g., `src/data_loader.py` and `src/helpers.py`).
- **Configuration Management:** File paths and other configuration parameters are managed via environment variables.
- **Logging:** The pipeline uses Python's built-in logging to track processing steps and facilitate debugging.
- **Testing:** A basic test suite is provided using `pytest` to ensure the functions work as expected.



## Setup Instructions

### 1. Clone the Repository

Clone this repository to your local machine using:

```bash
git clone https://github.com/your_username/your_project_name.git
cd your_project_name
```


### 2. Virtual Env Setup
```bash
python -m venv venv
```
# Activate the virtual environment:
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

# .env file

# Path to the 'delitos_por_municipio.csv' file (crime data)
DELITOS_FILE=./data/delitos_por_municipio.csv

# Path to the 'contac_center_data.csv' file (contact center data)
CONTACT_FILE=./data/contac_center_data.csv

# Path to the 'renta_por_hogar.csv' file (income data)
RENTA_FILE=./data/renta_por_hogar.csv


### 4. Run the Pipeline
```bash
python main.py
```

This will:

- Load the environment variables.

- Create an instance of DataLoader and load the delitos, contact, and renta datasets.

- Merge the renta and delitos datasets on the municipality field.

- Merge the result with the contact dataset on the CP (postal code).

- Log the progress and output the merged data.


### 5. Testing 
```bash
pytest
```
