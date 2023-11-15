'''
1. Download the current 'Agencia' file from the BCB website
2. Data Cleansing of 'Agencia' file in order to ingest the data on Databricks
3. Data Preprocessing (from .xlsx to .csv)
'''
import requests
import datetime
import zipfile
import pandas as pd
from unidecode import unidecode

def file_name():
    # Get the current date, year and month
    current_date = datetime.date.today()
    current_year = current_date.year
    current_month = current_date.month
    # Subtract 2 from the current month and calculate the result year
    result_month = current_month - 2
    result_year = current_year
    # Handle the case where subtracting 2 months would go back to the previous year
    if result_month < 1:
        result_month += 12  # Wrap around to December
        result_year -= 1   # Subtract 1 from the year
    if 1 <= result_month <= 9:
        result_month = f'0{result_month}'
    file_name = (str(result_year)+str(result_month))
    return file_name

def download_file(file_name):
    url = 'https://www.bcb.gov.br/fis/info/cad/agencias/'+file_name+'AGENCIAS.zip'
    # Define the file name you want to save the zip file as
    file_name_zip = (file_name+'AGENCIAS.zip')
    # Send an HTTP GET request to the URL
    response = requests.get(url)
    return response, file_name_zip

def check_downloaded_file(response, file_name_zip):
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the file in binary write mode and write the content to it
        with open(file_name_zip, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {file_name_zip} successfully!")
        with zipfile.ZipFile(file_name_zip, 'r') as zip_ref:
            zip_ref.extractall('.')
        print(f"ZIP file {file_name_zip} extracted successfully!")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

def xlsxtocsv(file_name):
    df = pd.read_excel(f'{file_name}AGENCIAS.xlsx', header=9)
    df.columns = df.columns.str.replace(' ', '')
    df = df[df['UF'].notna()]
    df.columns = [unidecode(col) for col in df.columns]
    df.columns = [col.upper() for col in df.columns]
    for col in list(df):
        try:
            df[col] = df[col].str.strip()
            df[col] = df[col].apply(unidecode)
        except:
            pass
    df = df.applymap(lambda x: x if not isinstance(x, str) else x.upper())
    df.to_csv('AGENCIAS.csv', index=False)

def main():
    filename = file_name()
    status, file_name_zip = download_file(filename)
    check_downloaded_file(status, file_name_zip)
    xlsxtocsv(filename)
    
if __name__ == '__main__':
    main()
