import logging
import os
import subprocess
from typing import Union

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession, session
from pyspark.sql.functions import (col)
from pyspark.sql.types import (ArrayType, BinaryType, BooleanType, DateType, DecimalType, FloatType, IntegerType,
                               MapType, StringType, StructField, StructType, TimestampType)


def call_bash():
    print('starting bash...')
    cwd = os.path.dirname(os.path.abspath(__file__))
    subprocess.call('./env.sh', shell=True)
    os.system('sh ./env.sh')
    print('ending bash...')


class SparkDataFrame:

    def __init__(self):
        self.set_java_home()
        self.set_log_level()

    @property
    def spark(self):
        return self.create_spark_session()

    @staticmethod
    def create_spark_session(app_name=None) -> session:
        return SparkSession.builder.appName(app_name).getOrCreate()

    @property
    def spark_context(self) -> SparkContext:
        return self.spark.sparkContext

    def set_log_level(self) -> None:
        # http://spark.apache.org/docs/latest/configuration.html#configuring-logging
        spark_logger = logging.getLogger('py4j')
        spark_logger.setLevel('ERROR')
        self.spark_context.setLogLevel(logLevel='FATAL')

    def read_file(self, file_path, options: dict = None) -> DataFrame:
        return self.spark.read.load(path=file_path, **options if options is not None else None)

    @staticmethod
    def save_file(df: DataFrame,
                  file_format: str,
                  file_path: str,
                  file_name: str,
                  options: dict = None) -> None:
        full_path = os.path.join(file_path, file_name + '{}'.format('.' + file_format))
        df.coalesce(1).write.format(file_format).save(path=full_path, mode='overwrite',
                                                      **options if options is not None else None)

    @staticmethod
    def set_java_home() -> None:
        java_location = r'C:\Program Files\Java\jdk1.8.0_241'
        os.environ['JAVA_HOME'] = java_location

    @staticmethod
    def create_schema(col_schema: dict) -> StructType:
        type_mapping = {
            'str': StringType(),
            'int': IntegerType(),
            'float': FloatType(),
            'bool': BooleanType(),
            'date': DateType(),
            'bytes': BinaryType()
        }
        schema = [StructField(col_name, type_mapping.get(field_type), True) for col_name, field_type in
                  col_schema.items()]
        return StructType(fields=schema)

    @staticmethod
    def print_schema(df: DataFrame):
        return df.printSchema()

    @staticmethod
    def select_columns(df: DataFrame, cols: list) -> DataFrame:
        new_df = df.select(cols)
        return new_df

    @staticmethod
    def rename(df: DataFrame, existing: str, new: str) -> DataFrame:
        new_df = df.WithColumnRenamed(existing, new)
        return new_df

    @staticmethod
    def rename_col_n_drop(df: DataFrame, existing: str, new: str) -> DataFrame:
        new_df = df.select('*', col(existing).alias(new)).drop(existing)
        return new_df

    @staticmethod
    def rename_multiple_cols(df: DataFrame, name_dict: dict) -> DataFrame:
        new_df = df.select([col(old_name).alias(new_name) for old_name, new_name in name_dict.items()])
        return new_df

    @staticmethod
    def rename_all_cols(df: DataFrame, new_name_list: list) -> DataFrame:
        existing_cols = df.columns
        assert len(existing_cols) == len(new_name_list), \
            '{} columns passed, original dataframe data had {} columns'.format(len(new_name_list),
                                                                               len(existing_cols))
        if len(existing_cols) == len(new_name_list):
            zipped = zip(existing_cols, new_name_list)
            return df.select([col(old_name).alias(new_name) for old_name, new_name in zipped])
        else:
            raise AssertionError

    @staticmethod
    def create_temp_view(df: DataFrame, table_name: str) -> None:
        df.createOrReplaceTempView(table_name)

    def execute_sql_query(self, query: str) -> DataFrame:
        return self.spark.sql(sqlQuery=query)

    @staticmethod
    def search_col(df: DataFrame, col_name, search_str) -> DataFrame:
        return df.filter(col(col_name).contains(search_str))

    @staticmethod
    def cast(df: DataFrame, col_name: str, data_type: Union[str, StringType, IntegerType,
                                                            FloatType, BooleanType,
                                                            BinaryType, DateType, DecimalType,
                                                            TimestampType, ArrayType, MapType]):
        return df.withColumn(col_name, col(col_name).cast(dataType=data_type))


class SparkRDD:

    # https://spark.apache.org/docs/latest/rdd-programming-guide.html

    def __init__(self, app_name=None):
        self.conf = SparkConf().setAppName(app_name if app_name is not None else '').setMaster('local[*]')
        self.sc = SparkContext(conf=self.conf)

    def get_lines(self, file_uri, min_partitions=None):
        return self.sc.textFile(name=file_uri, minPartitions=min_partitions).cache()

    @staticmethod
    def split_lines(rdd):
        return rdd.flatMap(lambda line: line.split(" "))
