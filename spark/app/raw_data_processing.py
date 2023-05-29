import os
import sys
from datetime import datetime
from pyspark.sql.functions import lit
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import element_at, split, col
from pyspark.sql import SparkSession

# Get the second argument passed to spark-submit (the first is the python app)
type = sys.argv[1]

spark = SparkSession.builder.appName("raw_data_processing_"+type).config("spark.sql.parquet.compression.codec", "lz4").getOrCreate()

input_path = "/usr/local/spark/data"
stage_path = "/usr/local/spark/staging"
today = datetime.today().strftime('%Y%m%d')

# Set the input paths
output_path = stage_path + "/" + today + "/raw_data_processing/" + type

# Read the CSV files into dataframes
symbols_valid_meta = spark.read.csv(os.path.join(input_path, "symbols_valid_meta.csv"), header='true')
symbols_valid_meta = symbols_valid_meta.select(['Symbol', 'Security Name'])

full_path = os.path.join(input_path, type, '*.csv')
stock_df = spark.read.format("csv").load(full_path, header='true')

stock_df = stock_df.withColumn("filename", input_file_name())

stock_df = stock_df.withColumn('filename', split(stock_df.filename, '/'))
stock_df = stock_df.withColumn('filename', element_at(col('filename'), -1) )
stock_df = stock_df.withColumn('Symbol_', split(stock_df.filename, '\.')[0])
stock_df = stock_df.drop('filename')

columns = ['Symbol', 'Security Name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
stock_df = stock_df.join(symbols_valid_meta, stock_df.Symbol_ ==  symbols_valid_meta.Symbol,"left").drop('Symbol_')
stock_df = stock_df.select(columns)

print(output_path+".parquet")
stock_df.write.mode('overwrite').option('compression', 'lz4').parquet(output_path+".parquet")

