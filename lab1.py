import requests
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.sql import select


class DBWeatherManager:
    def __init__(self, url):
        eng = create_engine(url)
        md = MetaData()
        md.create_all(eng)
        self.table = Table(
            'weather',
            md,
            Column('date', String),
            Column('mint', Float),
            Column('maxt', Float),
            Column('location', String),
            Column('humidity', Float),
        )
        self.conn = eng.connect()

    def insert(self, data: list):
        try:
            self.conn.execute(self.table.insert(), data)
        except Exception as ex:
            print("Error while write data in weather database")

    def select_all(self):
        try:
            return self.conn.execute(select([self.table]))
        except Exception as ex:
            print("Error while read data from weather database")
            return []


class WeatherProvider:
    def __init__(self, key):
        self.key = key

    def get_data(self, location, start_date, end_date):
        url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'
        params = {
            'aggregateHours': 24,
            'startDateTime': f'{start_date}T00:0:00',
            'endDateTime': f'{end_date}T23:59:59',
            'unitGroup': 'metric',
            'location': location,
            'key': self.key,
            'contentType': 'json',
        }
        data = requests.get(url, params).json()
        return [
            {
                'date': row['datetimeStr'][:10],
                'mint': row['mint'],
                'maxt': row['maxt'],
                'location': 'Volgograd,Russia',
                'humidity': row['humidity'],
            }
            for row in data['locations'][location]['values']
        ]


db = DBWeatherManager('sqlite:///weather.sqlite3')
provider = WeatherProvider('4978GENE8GJ3U5X4LBT2R9LBU')
db.insert(provider.get_data('Volgograd,Russia', '2020-09-20', '2020-09-29'))

for row in db.select_all():
    print(row)
