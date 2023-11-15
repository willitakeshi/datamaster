'''
1. Download the current 'IBGE' file from the IBGE website
2. Data Cleansing of 'IBGE' file in order to ingest the data on Databricks
3. Data Preprocessing (from .xls to .csv)
'''
import pandas as pd
import requests
from unidecode import unidecode

url = 'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Previa_da_Populacao/POP2022_Municipios_20230622.xls'
response = requests.get(url)
if response.status_code == 200:
    # Open the file in binary write mode and write the content to it
    with open('IBGE.xls', 'wb') as file:
        file.write(response.content)
    print(f"Downloaded successfully!")
else:
    print(f"Failed to download the file. Status code: {response.status_code}")

def xlsxtocsv():
    df = pd.read_excel(f'IBGE.xls', header=1)
    
    df.columns = df.columns.str.replace(' ', '')
    df.columns = df.columns.str.replace('.', '')
    df['CODUF'] = df['CODUF'].astype(str).str.replace('.0', '', regex=False)
    df['CODMUNIC'] = df['CODMUNIC'].astype(str).str.replace('.0', '', regex=False)
    df = df[df['NOMEDOMUNICÍPIO'].notna()]
    df.columns = [unidecode(col) for col in df.columns]
    df.columns = [col.upper() for col in df.columns]
    for col in list(df):
        try:
            df[col] = df[col].apply(unidecode)
        except:
            pass
    df = df.applymap(lambda x: x if not isinstance(x, str) else x.upper())
    df.to_csv('IBGE.csv', index=False)

xlsxtocsv()
