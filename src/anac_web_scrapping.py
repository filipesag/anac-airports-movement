from io import StringIO
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import pandas as pd
import os


url = "https://www.gov.br/anac/pt-br/acesso-a-informacao/dados-abertos/areas-de-atuacao/operador-aeroportuario/dados-de-movimentacao-aeroportuaria/metadados-operador-aeroportuario-dados-de-movimentacao-aeroportuaria"
output_filename = "iata_service_type.csv"
output_dir = "./data/raw/anac_web_scrapping"


os.makedirs(output_dir, exist_ok=True)

option = Options()
option.headless = True

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=option)
driver.get(url)

try:
    button_cookies = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Aceitar')]"))
    )
    button_cookies.click()

    tables = driver.find_elements(By.TAG_NAME, "table")
    services_type_table = tables[2]
    html_content = services_type_table.get_attribute('outerHTML')

    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find(name='table')
    
    df = pd.read_html(StringIO(str(table)))[0]
    
    df.to_csv(os.path.join(output_dir, output_filename), index=False, header=False)
    print(f"File saved successfully to {os.path.join(output_dir, output_filename)}")

except Exception as e:
    raise
finally:
    driver.quit() 