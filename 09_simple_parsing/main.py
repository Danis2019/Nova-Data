from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
from s3_object_client import AsyncObjectStorage
import asyncio
from dotenv import load_dotenv
import os

# Retry policy configs
MAX_TRIES = 5
SLEEP_AT_START = 0
SLEEP_MULTIPLIER = 2

# Url template to get some page from books.toscrape.com
URL_TEMPLATE = 'https://books.toscrape.com/catalogue/page-{}.html'


def get_page_using_retry_policy(url: str) -> requests.Response | None:
    current_sleep_time: int = SLEEP_AT_START

    for _ in range(MAX_TRIES):
        time.sleep(current_sleep_time)
        page = requests.get(url)
        if page.status_code == 200:
            print("status_code ok!")
            return page
        else:
            print("Error!")
            current_sleep_time += SLEEP_MULTIPLIER
            print(f"Go sleep {current_sleep_time} seconds!")
    print("Max attemtps, return None")
    return None

def parse_books_data(page: requests.Response, df: pd.DataFrame) -> pd.DataFrame:
    soup = BeautifulSoup(page.text, "html.parser")
    allbooks = soup.find('ol', class_='row')
    allbooks_array = allbooks.find_all(class_='col-xs-6 col-sm-4 col-md-3 col-lg-3')
    for book in allbooks_array:
        name = book.find('h3').a['title']
        price = book.find(class_='price_color').text
        isin_stock = book.find(class_='instock availability').get_text(strip=True)
        new_row = pd.Series([name, float(price[1:]), isin_stock], index=df.columns)
        df.loc[len(df)] = new_row
    return df

def send_books_data_to_s3(file_name: str):

    load_dotenv()

    YOUR_ACCESS_KEY = os.getenv('YOUR_ACCESS_KEY')
    YOUR_SECRET_KEY = os.getenv('YOUR_SECRET_KEY')

    storage = AsyncObjectStorage(
        key_id= YOUR_ACCESS_KEY,
        secret= YOUR_SECRET_KEY,
        endpoint="https://s3.ru-7.storage.selcloud.ru",
        container='data-engineer-practice-dbatyrshin'
    )
    asyncio.run(storage.send_file(file_name))

if __name__ == '__main__':
    
    df = pd.DataFrame(columns=['Name', 'Price', 'IsStock'])

    SLEEP_BETWEEN_PAGES = 2
    PAGE_COUNT = 50
    for page_number in range(1, PAGE_COUNT + 1):
        url = URL_TEMPLATE.format(page_number)
        print(url)
        page = get_page_using_retry_policy(url)

        if page != None:
            page.encoding = 'utf-8'
            df = parse_books_data(page, df)

        time.sleep(SLEEP_BETWEEN_PAGES)

    median_books_price = df['Price'].median()

    file_name = 'filetered_books_data.csv'

    filtered_df = df[df['Price'] > median_books_price]
    filtered_df.to_csv(file_name, index=False)

    send_books_data_to_s3(file_name)
