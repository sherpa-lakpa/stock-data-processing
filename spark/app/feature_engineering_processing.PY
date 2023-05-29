import os
import sys
from datetime import datetime
from pyspark.sql.functions import col, avg, expr
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# Get the second argument passed to spark-submit (the first is the python app)
type = sys.argv[1]

spark = SparkSession.builder.appName("feature_engineering_processing_"+type).\
        config("spark.sql.parquet.compression.codec", "lz4").\
        config("spark.reducer.maxReqsInFlight", "10"). \
        config("spark.shuffle.io.retryWait", "60s"). \
        config("spark.shuffle.io.maxRetries", "10").getOrCreate()
# config("spark.executor.memory", "3g"). \

today = datetime.today().strftime('%Y%m%d')
raw_data_processing_path = "/usr/local/spark/staging/" + today + "/raw_data_processing/"
output_path = "/usr/local/spark/staging/" + today + "/feature_engineering/"

full_path = os.path.join(raw_data_processing_path, type+'.parquet', '*.parquet')
stock_df = spark.read.format("parquet").load(full_path)

# Define the window specification partitioned by the stock symbol and ordered by the date
window_spec = Window.partitionBy("Symbol").orderBy("Date")

# Calculate the rolling average of the trading volume (Volume)
df_with_rolling_avg = stock_df.withColumn("vol_moving_avg", avg(col("Volume")).over(window_spec))

# Calculate the rolling median of the Adjusted Close (Adj Close)
df_with_rolling_avg_and_median = df_with_rolling_avg.withColumn("adj_close_rolling_med", expr("percentile_approx(`Adj Close`, 0.5)").over(window_spec))

print(output_path+type+".parquet")
df_with_rolling_avg_and_median.write.mode('overwrite').option('compression', 'lz4').parquet(output_path+type+".parquet")