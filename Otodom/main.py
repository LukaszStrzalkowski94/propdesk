from otodom_scraper import scrape_mieszkania
import json
import pandas as pd
from otodom_cleaner import clean_data  # musimy dodać taką funkcję w cleanerze

def main():
    num_pages = 593

    # Krok 1: Scrapowanie danych
    wszystkie_mieszkania = scrape_mieszkania(num_pages)
    print(f"Pobrano {len(wszystkie_mieszkania)} mieszkań.")

    # Zapis danych do pliku JSON
    json_filename = f"Otodom_links_page_{num_pages}.json"
    with open(json_filename, "w") as f:
        json.dump(wszystkie_mieszkania, f)
    print(f"Dane zapisane do pliku {json_filename}")

    # Wczytaj dane i przetwórz
    df = pd.read_json(json_filename)
    df_cleaned = clean_data(df)  # wywołanie funkcji czyszczącej

    # Zapis do CSV
    csv_filename = f"Otodom_page_{num_pages}.csv"
    df_cleaned.to_csv(csv_filename, index=False)
    print(f"Przetworzone dane zapisane do pliku {csv_filename}")

if __name__ == "__main__":
    main()