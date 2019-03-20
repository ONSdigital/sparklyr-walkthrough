import pandas as pd
from pathlib import Path


THIS_DIR = Path(__file__).parent
DATA_DIR = THIS_DIR.parent / 'data'


def clean_rescue_data():
    
    input_datapath = DATA_DIR / 'raw' / 'animal-rescue.csv'
    output_datapath = DATA_DIR / 'clean' / 'animal-rescue.csv'
    
    raw_df = pd.read_csv(str(datapath), encoding='cp1255')
    raw_df.to_csv(output_datapath, index=False)
    

def clean_population_data():

    input_datapath = DATA_DIR / 'raw' / 'Postcode_Estimates_Table_1.csv'
    output_datapath = DATA_DIR / 'clean' / 'population_by_postcode.csv'

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

    df.to_csv(output_datapath, index=False)


if __name__ == '__main__':
    
    clean_rescue_data()
    clean_population_data()

    # Transfer to HDFS
    !hdfs dfs -put -f data/clean/animal-rescue.csv /tmp/training/animal-rescue.csv
    !hdfs dfs -put -f data/clean/population_by_postcode.csv /tmp/training/population_by_postcode.csv
    
