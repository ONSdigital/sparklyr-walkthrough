import pandas as pd
from pathlib import Path


def clean_rescue_data(data_dir):
    
    input_datapath = data_dir / 'raw' /'animal-rescue.csv'
    output_datapath = data_dir / 'clean' / 'animal-rescue.csv'
    
    raw_df = pd.read_csv(str(input_datapath), encoding='cp1255')
    raw_df.to_csv(str(output_datapath), index=False)
    
    return str(output_datapath)
    

def clean_population_data(data_dir):

    input_datapath = data_dir / 'raw' / 'Postcode_Estimates_Table_1.csv'
    output_datapath = data_dir / 'clean' / 'population-by-postcode.csv'

    df = pd.read_csv(input_datapath)

    df['OutwardCode'] = df.Postcode.str.slice(start=0, stop=-3)
    df['OutwardCode'] = df.OutwardCode.str.strip()

    df['InwardCode'] = df.Postcode.str.slice(start=-3)

    col_order = [
        'Postcode', 
        'OutwardCode', 
        'InwardCode',
        'Females', 
        'Males', 
        'Total', 
        'Occupied_Households',
    ]

    df = df[col_order]

    df.to_csv(str(output_datapath), index=False)
    
    return str(output_datapath)

