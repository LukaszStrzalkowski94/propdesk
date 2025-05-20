from otodom_scraper import scrape_mieszkania
import pandas as pd
import json

def clean_data(df):
    df['portal_name'] = 'Otodom'
    df = df[[
            'owner_id',
            'id', 
            'agency_id', 
            'Building_type', 
            'Price',
            'Price_per_m', 
            'Area', 
            'Floor_no', 
            'Building_floors_num',
            'Rooms_num', 
            'Building_ownership',  
            'Build_year', 
            'Building_material',
            'Heating',
            'Extras_types',
            'Windows_type',
            'Security_types',
            'Rent',
            'createdAt',
            'modifiedAt',
            'district_name',
            'street',
            'slug',
            'market',
            'City',
            'district_name',
            'street',
            'portal_name',
            'ProperType'                       ]].rename(columns={'slug': 'url',
                                            'Price':'price',
                                            'market': 'market_type',
                                            'district_name':'district',
                                            'Area':'total_area',
                                            'Floor_no':'property_level',
                                            'Building_floors_num':'total_property_level',
                                            'Rooms_num':'no_rooms',
                                            'Building_ownership':'ownership_form',
                                            'Building_type':'building_type',
                                            'Build_year':'building_year',
                                            'Building_material':'material',
                                            'lift':'elevator',
                                            'Rent':'property_rent',
                                            'market':'market_type',
                                            'Price_per_m':'row_price_m2',
                                            'separate_kitchen':'kitchen_type',
                                            'Heating':'heating_type',
                                            'Windows_type':'windows',
                                            'Security_types':'security',
                                            'modifiedAt': 'date_update',
                                            'createdAt':'date_added',
                                            'agency_id': 'id_realestateagency',
                                            'City': 'city',
                                            'district_name':'district',
                                            'ProperType':'property_type'
                                            })

    columns_to_string = ['heating_type', 'ownership_form','building_type','material','windows',]
    # Zastosuj funkcję lambda do każdej kolumny indywidualnie
    for columns in columns_to_string:
        df[columns] = df[columns].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        
    # Funkcja do tworzenia kolumn na podstawie warunków
    def extract_feature(extras, condition, feature_name):
        if extras is not None and condition(extras):
            return feature_name
        else:
            return None
        
    # Tworzenie kolumny "elevator" na podstawie kolumny "Extras_types"
    df['elevator'] = df['Extras_types'].apply(lambda x: extract_feature(x, lambda extras: any('lift' in item for item in extras), 1))
    # Tworzenie kolumny "kitchen_type" na podstawie kolumny "Extras_types"
    df['kitchen_type'] = df['Extras_types'].apply(lambda x: extract_feature(x, lambda extras: 'separate_kitchen' in extras, 'separate kitchen'))  
    #Tworzenie kolumnt balcon/logia jezeli w extras jest balcon, lub taras,
    df['balcony/loggia'] = df['Extras_types'].apply(lambda x: extract_feature(x, lambda extras: 'terrace' or 'balcony' in extras, 1))  
    #Tworzenie kolumny parking_type, jezeli jest garaz to garaz 
    df['parking_type'] = df['Extras_types'].apply(lambda x: extract_feature(x, lambda extras: 'garage' in extras, 'garage'))  
    #Tworzenie kolumny basement, jezeli jest basement to basement 
    df['basement'] = df['Extras_types'].apply(lambda x: extract_feature(x, lambda extras: 'basement' in extras, 'basement'))  
    #porządkowanie listy 
    df = df[['id',
            'id_realestateagency',
            'date_added',
            'date_update',
            'url',
            'portal_name',
            'market_type',
            'property_type',
            'price',
            'row_price_m2',
            'city',
            'district',
            'street',
            'total_area',
            'property_level',
            'total_property_level',
            'no_rooms',
            'kitchen_type',
            'ownership_form',
            'building_type',
            'building_year',
            'material',
            'heating_type',
            'elevator',
            'heating_type',
            'windows',
            'balcony/loggia',
            'parking_type',
            'basement',
            'security',
            'property_rent']]
    print("Zapisano plik CSV w bieżącym folderze.")
    return df
