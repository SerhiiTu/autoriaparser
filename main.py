import csv
import random
from time import sleep
import sqlite3

import requests
from bs4 import BeautifulSoup


def random_sleep():
    sleep(random.randint(2, 5))


def get_page_content(page: int, page_size: int = 100):
    base_url = 'https://auto.ria.com/uk/search/'
    query_params = {
        'indexName': 'auto',
        'categories.main.id': '1',
        'country.import.usa.not': '-1',
        'price.currency': '1',
        'abroad.not': '0',
        'custom.not': '1',
        'page': page,
        'size': page_size,
    }

    response = requests.get(base_url, params=query_params)
    response.raise_for_status()
    return response.text


def get_details_page_content(url: str):
    base_url = f'https://auto.ria.com/uk{url}'

    response = requests.get(base_url)
    response.raise_for_status()
    return response.text


class CSVWriter:
    def __init__(self, file_name: str, headers: list):
        self.file_name = file_name

        with open(self.file_name, 'w') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

    def write_data(self, data):
        with open(self.file_name, 'a') as file:
            writer = csv.writer(file)
            writer.writerow(data)


class SqliteWriter:
    def __init__(self, table_name: str, headers: list):
        self.table_name = table_name
        self.connection = sqlite3.connect("database.db")
        self.cursor = self.connection.cursor()
        self.headers = headers
        execution = f'''
            CREATE TABLE IF NOT EXISTS Cars (
            id INTEGER PRIMARY KEY,
            {self.headers[0]} TEXT,
            {self.headers[1]} TEXT,
            {self.headers[2]} TEXT,
            {self.headers[3]} TEXT,
            {self.headers[4]} TEXT,
            {self.headers[5]} TEXT
            )
            '''
        self.cursor.execute(execution)
        self.connection.commit()
        self.connection.close()

    def write_data(self, data):
        self.connection = sqlite3.connect("database.db")
        self.cursor = self.connection.cursor()
        execution = f'''
                    INSERT INTO Cars ({self.headers[0]}, {self.headers[1]}, {self.headers[2]}, {self.headers[3]}, {self.headers[4]}, {self.headers[5]}) 
                    VALUES ('{data[0]}', '{data[1]}', '{data[2]}', '{data[3]}', '{data[4]}', '{data[5]}');
                    '''
        self.cursor.execute(execution)
        self.connection.commit()
        self.connection.close()


class StdoutWriter:
    def write_data(self, data):
        print(data)


def check_db():  # for dev
    connection = sqlite3.connect("database.db")
    cursor = connection.cursor()
    execution = "SELECT * FROM Cars"
    result = cursor.execute(execution)
    result = cursor.fetchall()
    connection.commit()
    connection.close()
    return result


def clear_db(): # for dev
    connection = sqlite3.connect("database.db")
    cursor = connection.cursor()
    execution = "DELETE FROM Cars"
    cursor.execute(execution)
    connection.commit()
    connection.close()

def main():
    page = 0
    headers = ['car_id', 'mark', 'model', 'year', 'price', 'link']

    writers = (
        CSVWriter('cars.csv', headers),
        SqliteWriter('Cars', headers)
    )

    while True:
        # random_sleep()
        if page > 5:
            break

        print(f"Processing page {page}!")

        page_content = get_page_content(page)

        soup = BeautifulSoup(page_content, 'html.parser')
        search_results = soup.find('div', id="searchResults")
        ticket_items = search_results.find_all("section", class_="ticket-item")

        if not ticket_items:
            print(f"No more items on page {page}!")
            break

        for ticket_item in ticket_items:
            car_details = ticket_item.find("div", class_="hide")
            car_id = car_details['data-id']
            car_mark_details = car_details['data-mark-name']
            car_model_name = car_details['data-model-name']
            car_year = car_details['data-year']
            car_link_to_view = car_details['data-link-to-view']

            details_page_content = get_details_page_content(car_link_to_view)
            soup2 = BeautifulSoup(details_page_content, 'html.parser')
            price_section = soup2.find('section', class_="price mb-15 mhide")
            car_price = price_section.find('strong').contents[0]

            data = [car_id, car_mark_details, car_model_name, car_year, car_price, car_link_to_view]

            for writer in writers:
                writer.write_data(data)

        page += 1


if __name__ == '__main__':
    main()
    # clear_db()
    # print(check_db())

