import requests
from bs4 import BeautifulSoup
import pandas as pd 
import time
import json
from tqdm import tqdm


#Funkcja do otwarcia url i zamiana na jason
def get_json(url, max_retries = 3):
    """Pobiera dane JSON z podanego URL"""
    retries = 0
    while retries < max_retries:
        try:
            headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"}
            resp = requests.get(url,headers=headers)
            soup = BeautifulSoup(resp.text,'html.parser')
            data_tag = soup.find('script',{'id':'__NEXT_DATA__'}).text
            
            if data_tag is None:
                raise ValueError("Nie znaleziono odpowiedniego tagu <script> na stronie.")
            
            data = json.loads(data_tag)
            return data 
        
        except (Exception, requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f'Błąd: {e}')
            print(f'Ponawiam próbę po 5 sekundach...')
            time.sleep(5)
            retries += 1

def pages_to_scrape(pages):
    lista_url = []
    for page in tqdm(range(1, pages + 1), desc='Scraping pages'):
        search_url = f'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/mazowieckie/warszawa/warszawa/warszawa?viewType=listing&page={page}'
        search_data = get_json(search_url)
        items = search_data['props']['pageProps']['data']['searchAds']['items']
        print(f"Na stronie {page} jest tyle linków: {len(items)}")
        for prop in items:
            prop_url = 'https://www.otodom.pl/pl/oferta/' + prop['slug']
            lista_url.append(prop_url)

    print(f'Skończone, linków na {pages} stronach jest {len(lista_url)}')
    return lista_url


def extract_data(search_data):
    """Ekstrahuje dane z JSON i zwraca wypełniony słownik mieszkanie."""
    klucze_target = ['Area', 'Build_year', 'Building_floors_num', 'Building_type', 'Building_ownership', 'Construction_status', 'City', 'City_id', 'Country', 'Extras_types', 'Floor_no', 'MarketType', 'OfferType', 'Price', 'Price_per_m', 'ProperType', 'Province', 'Rent', 'Rooms_num', 'Subregion', 'Windows_type', 'env', 'Equipment_types', 'Heating', 'Media_types', 'Security_types', 'Building_material', 'user_type']
    klucze_ad = ['id', 'market', 'slug', 'advertType', 'createdAt', 'modifiedAt', 'description', 'exlusiveOffer']
    klucze_owner = ['id', 'name', 'phones']
    klucze_agency = ['id', 'name']
    klucze_location_coordinates = ['latitude', 'longitude']
    klucze_district = ['code', 'id', 'name']
    mieszkanie = {}
    
    # Pętla dla kluczy w sekcji 'target'
    for klucz in klucze_target:
        mieszkanie[klucz] = search_data['props']['pageProps']['ad']['target'].get(klucz)

    # Pętla dla kluczy w sekcji 'ad'
    for klucz in klucze_ad:
        mieszkanie[klucz] = search_data['props']['pageProps']['ad'].get(klucz)

    # Pętla dla kluczy w sekcji 'owner'
    for klucz in klucze_owner:
        mieszkanie['owner_' + klucz] = search_data['props']['pageProps']['ad']['owner'].get(klucz)

    # Pętla dla kluczy w sekcji 'agency'
    if search_data['props']['pageProps']['ad']['agency'] is not None:
        for klucz in klucze_agency:
            mieszkanie['agency_' + klucz] = search_data['props']['pageProps']['ad']['agency'].get(klucz)

    # Pętla dla kluczy w sekcji 'coordinates'
    for klucz in klucze_location_coordinates:
        mieszkanie[klucz] = search_data['props']['pageProps']['ad']['location']['coordinates'].get(klucz)

    # Pętla dla kluczy w sekcji 'district'
    if search_data['props']['pageProps']['ad']['location']['address']['district'] is not None:
        for klucz in klucze_district:
            mieszkanie['district_' + klucz] = search_data['props']['pageProps']['ad']['location']['address']['district'].get(klucz)
    
    if search_data['props']['pageProps']['ad']['location']['address']['street'] is not None:
        mieszkanie['street'] = search_data['props']['pageProps']['ad']['location']['address']['street']['name']

    return mieszkanie

def scrape_mieszkania(num_pages):
    pages = pages_to_scrape(num_pages)
    wszystkie_mieszkania = []

    for link in tqdm(pages, desc='Downloading information from links'):
        search_data = get_json(link)
        mieszkanie = extract_data(search_data)
        wszystkie_mieszkania.append(mieszkanie)
    
    return wszystkie_mieszkania
