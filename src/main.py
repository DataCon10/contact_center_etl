from data_loader import load_delitos_df, load_contact_df, load_renta_df

def main():
    # Load datasets using the functions defined in data_loader.py
    delitos_df = load_delitos_df("data/delitos_por_municipio.csv")
    contact_df = load_contact_df("data/contac_center_data.csv")
    renta_df   = load_renta_df("data/renta_por_hogar.csv")  


if __name__ == "__main__":
    main()