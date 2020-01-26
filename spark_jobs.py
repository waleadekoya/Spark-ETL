from PySpark_ETL.spark_utils import SparkDataFrame, SparkRDD

from pyspark.sql.functions import (col)

from threading import Thread
import os

print(os.path.dirname(os.path.abspath(__file__)))


class AirportsQueries(SparkDataFrame):

    def __init__(self, path):
        super().__init__()
        self.path = path
        self.headers = ['airport_id',
                        'airport_name',
                        'city_name',
                        'country_of_airport',
                        'IATA_FAA_code',
                        'ICAO_code',
                        'latitude',
                        'longitude',
                        'altitude',
                        'TBC',
                        'timezone_DST',
                        'timezone_in_Oslo']
        self.airports = self.rename_all_cols(self.get_file_extract(), self.headers).cache()
        self.generate_outputs()

    def get_file_extract(self):
        airports = self.read_file(file_path=self.path, options=dict(format='csv', header='false', sep=','))
        return self.rename_all_cols(airports, self.headers)

    def get_us_airports(self):
        return self.airports.filter(col('country_of_airport').contains('United States')). \
            select(['airport_name', 'city_name']).drop_duplicates()

    @property
    def us_airports(self):
        return self.get_us_airports()

    @property
    def airports_by_latitudes(self):
        return self.get_latitudes()

    def get_latitudes(self):
        return self.airports.filter(col('latitude') > 40).select(['airport_name', 'latitude']).drop_duplicates()

    def generate_outputs(self):
        Thread(target=self.save_file,
               args=(self.us_airports, 'csv', os.getcwd(), 'us_airports', dict(header='true'),)).start()

        Thread(target=self.save_file,
               args=(self.airports_by_latitudes, 'csv', os.getcwd(), 'airports_over_lat_40', dict(header='true'),)).start()


class WordCount(SparkRDD):

    def __init__(self, path):
        super().__init__()
        self.path = path
        self.process_text_file()

    def process_text_file(self):
        lines = self.get_lines(file_uri=self.path)
        word_count = self.split_lines(lines).map(lambda x: str(x).lower()).countByValue()
        [print('{}: {}'.format(word, count)) for word, count in word_count.items()]


if __name__ == '__main__':
    file_path = 'word_count.text '
    jobs = WordCount(file_path)
