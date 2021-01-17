import requests
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.sql import select


class DBManagerInterface:
    def __init__(self, url):
        self.eng = create_engine(url)
        self.md = MetaData()
        self.md.create_all(self.eng)
        self.conn = self.eng.connect()
        self.table = Table()

    def write(self, data: list):
        pass

    def read(self):
        pass


class DBWeatherManager (DBManagerInterface):
    def __init__(self, url):
        super().__init__(url)
        self.define_table()

    def define_table(self):
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
db.write(provider.get_data('Volgograd,Russia', '2020-09-20', '2020-09-29'))

for row in db.read():
    print(row)
