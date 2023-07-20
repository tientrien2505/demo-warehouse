from pyspark.sql import SparkSession, DataFrame
from sources import EMPLOYEE_SOURCE
from pyspark.sql import functions as F
from dataclasses import dataclass

import logging

logger = logging.getLogger(__name__)

@dataclass
class InputData:
    employee_df: DataFrame


def read_data(spark: SparkSession) -> InputData:
    logger.info(f'Loading data {EMPLOYEE_SOURCE}')
    employee_df = spark.read.parquet(EMPLOYEE_SOURCE)
    return InputData(
        employee_df=employee_df
    )


def process_data(input_data: InputData) -> DataFrame:
    logger.info(f'Processing employee dataframe')
    employee_df = input_data.employee_df
    processed_employee_df = (
        employee_df
        .withColumn('name_id', F.concat_ws('_', 'name', 'id'))
    )
    return processed_employee_df


def write_data(df: DataFrame) -> None:
    path = 's3://truongdt3-test/processed_employee/'
    logger.info(f'Writing df to {path}')
    df.write.parquet(
        path=path,
        mode='overwrite',
    )


if __name__ == '__main__':
    spark = SparkSession.Builder().getOrCreate()
    input_data = read_data(spark=spark)
    input_data.employee_df.show()
    df = process_data(input_data=input_data)
    df.show()
    write_data(df)