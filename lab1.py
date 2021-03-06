import requests
import argparse
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.sql import select


class DBManagerInterface:
    def __init__(self, url):
        self.eng = create_engine(url)
        self.md = MetaData()
        self.md.create_all(self.eng)
        self.conn = self.eng.connect()
        self.table = Table()

    def write(self, data: list) -> None:
        pass

    def read(self) -> list:
        pass


class DBWeatherManager (DBManagerInterface):
    def __init__(self, url):
        super().__init__(url)
        self.define_table()

    def define_table(self) -> None:
        self.table = Table(
            'weather',
            self.md,
            Column('date', String),
            Column('mint', Float),
            Column('maxt', Float),
            Column('location', String),
            Column('humidity', Float),
        )

    def write(self, data: list) -> None:
        try:
            self.conn.execute(self.table.insert(), data)
        except Exception as ex:
            print("Error while write data in weather database")

    def read(self) -> list:
        try:
            return self.conn.execute(select([self.table]))
        except Exception as ex:
            print("Error while read data from weather database")
            return []


class WeatherProviderInterface:
    def __init__(self, key):
        self.key = key

    def get_weather(self, location, start_date, end_date) -> list:
        pass


class WeatherVCProvider(WeatherProviderInterface):
    def __init__(self, key):
        super().__init__(key)
        self.url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'

    def get_weather(self, location, start_date, end_date) -> list:
        params = {
            'aggregateHours': 24,
            'startDateTime': f'{start_date}T00:0:00',
            'endDateTime': f'{end_date}T23:59:59',
            'unitGroup': 'metric',
            'location': location,
            'key': self.key,
            'contentType': 'json',
        }
        data = requests.get(self.url, params).json()
        return [
            {
                'date': row['datetimeStr'][:10],
                'mint': row['mint'],
                'maxt': row['maxt'],
                'location': location,
                'humidity': row['humidity'],
            }
            for row in data['locations'][location]['values']
        ]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get weather info')
    parser.add_argument('location', help='location for loading weather', type=str)
    parser.add_argument('start_date', help='start date of uploading information', type=str)
    parser.add_argument('end_date', help='end date of uploading information', type=str)
    parser.add_argument('--output', help='output file path', type=str)
    args = parser.parse_args()

    db = DBWeatherManager('sqlite:///weather.sqlite3')
    provider = WeatherVCProvider('4978GENE8GJ3U5X4LBT2R9LBU')
    db.write(provider.get_weather(args.location, args.start_date, args.end_date))

    weather = db.read()
    if args.output:
        with open(args.output, 'w') as f:
            for row in weather:
                f.write(str(row) + '\n')
    else:
        for row in weather:
            print(row)