import pandas as pd
df1 = pd.read_csv('rr.csv')
df2 = pd.read_csv('rrr.csv')
outfile = pd.merge(df1, df2, on='movieId')
outfile.to_csv('outfile.csv')