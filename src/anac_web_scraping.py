from io import StringIO
from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def scrape_iata_service_types():
    url = "https://www.gov.br/anac/pt-br/acesso-a-informacao/dados-abertos/areas-de-atuacao/operador-aeroportuario/dados-de-movimentacao-aeroportuaria/metadados-operador-aeroportuario-dados-de-movimentacao-aeroportuaria"
    output_filename = "iata_service_type.csv"
    output_dir = "./data/raw/anac_web_scraping"

    os.makedirs(output_dir, exist_ok=True)

    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')
        target_table = tables[2]
        
        df = pd.read_html(StringIO(str(target_table)), header=None)[0]
        
        df.columns = df.iloc[0].astype(str).str.strip()
        
        df = df[1:]
        
        df.columns = df.columns.str.replace(u'\u200b', '', regex=True)
        service_csv = df.to_csv(index=False, header=True)
        logging.info(f"File created")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

    return service_csv

