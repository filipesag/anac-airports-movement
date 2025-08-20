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

        df = pd.read_html(StringIO(str(target_table)))[0]
        
        service_csv = df.to_csv(index=False, header=False)
        logging.info(f"File saved in {os.path.join(output_dir, output_filename)}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

    return service_csv

if __name__ == "__main__":
    scrape_iata_service_types()
