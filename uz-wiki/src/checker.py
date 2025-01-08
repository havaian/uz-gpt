import pandas as pd
df = pd.read_csv('data/raw/batch_0.csv')
print(df.head())
print(f"Total articles scraped: {len(df)}")