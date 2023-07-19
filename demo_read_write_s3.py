from pyspark.sql import SparkSession, DataFrame
from sources import EMPLOYEE_SOURCE
from pyspark.sql import functions as F


class InputData:
    employee_df: DataFrame


def read_data(spark: SparkSession) -> InputData:
    employee_df = spark.read.parquet(EMPLOYEE_SOURCE)
    return InputData(
        employee_df=employee_df
    )


def process_data(input_data: InputData) -> DataFrame:
    employee_df = input_data.employee_df
    processed_employee_df = (
        employee_df
        .withColumn('name_id', F.concat_ws('_', 'name', 'id'))
    )
    return processed_employee_df


def write_data(df: DataFrame) -> None:
    df.write.parquet(
        path='s3a://warehouse3/processed_employee/',
        mode='overwrite',
    )


if __name__ == '__main__':
    spark = SparkSession.Builder().config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider").getOrCreate()
    input_data = read_data(spark=spark)
    df = process_data(input_data=input_data)
    write_data(df)