import pandas as pd 
import sqlite3
from datetime import datetime 

json_path = '/media/lucas/Files/2.Projetos/etl-scrapy-project/data/data.json'

df = pd.read_json(json_path, lines=False)


df['source'] = "https://lista.mercadolivre.com.br/barco-decorativo"

df['data_coleta'] = datetime.now()

pd.options.display.max_columns = None

df['old_price_reais'] = df['old_price_reais'].fillna(0).astype(float)
df['old_price_centavos'] = df['old_price_centavos'].fillna(0).astype(float)
df['new_price_reais'] = df['new_price_reais'].fillna(0).astype(float)
df['new_price_centavos'] = df['new_price_centavos'].fillna(0).astype(float)
df['reviews_rating_number'] = df['reviews_rating_number'].fillna(0).astype(float)
df['reviews_amount'] = df['reviews_amount'].replace('[\(\)]','',regex=True)
df['reviews_amount'] = df['reviews_amount'].fillna(0).astype(float)
df['old_price'] = (df['old_price_reais']+df['old_price_centavos']) / 100
df['new_price'] = (df['new_price_reais']+df['new_price_centavos']) / 100

df.drop(columns=['old_price_reais', 'old_price_centavos','new_price_reais','new_price_centavos'],inplace=True)


conn = sqlite3.connect("/media/lucas/Files/2.Projetos/etl-scrapy-project/data/quotes.db")

df.to_sql('mercadolive_items', conn, if_exists='replace', index=False)

conn.close()

print(df)
