import pandas as pd

csv = pd.read_csv('ratings_train.csv')
counted = csv.groupby(['movieId']).size()

s = counted.sort_values(ascending=False)
c = s[0:10000]
c.to_csv("rr.csv")