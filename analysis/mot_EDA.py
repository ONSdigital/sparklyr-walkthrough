import pandas as pd


reader = pd.read_csv('data/raw/test_result_2010.txt.gz', chunksize=1000, sep='|')

df = next(reader)

df = 