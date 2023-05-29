import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("train_model").config("spark.sql.parquet.compression.codec", "lz4").getOrCreate()

today = datetime.today().strftime('%Y%m%d')
feature_engineering_path = "/usr/local/spark/staging/" + today + "/feature_engineering/"

full_path = os.path.join(feature_engineering_path, '*', '*.parquet')
stock_df = spark.read.format("parquet").load(full_path)

stock_df = stock_df.dropna()

feature_df = stock_df.selectExpr("cast(vol_moving_avg as float) vol_moving_avg", 
                    "cast(adj_close_rolling_med as float) adj_close_rolling_med", 
                    "cast(Volume as float) Volume")
vectorAssembler = VectorAssembler(inputCols = ['vol_moving_avg', 'adj_close_rolling_med'], outputCol = 'features')
vstock_df = vectorAssembler.transform(feature_df)
vstock_df = vstock_df.select('features', 'Volume')

splits = vstock_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

lr = RandomForestRegressor(featuresCol = 'features', labelCol='Volume')
lr_model = lr.fit(train_df)

lr_predictions = lr_model.transform(test_df)

lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="Volume")
print("MAE on test data = %g" % lr_evaluator.evaluate(lr_predictions, {lr_evaluator.metricName: "mae"}))

model_path = f"/usr/local/spark/staging/regressor.model"
lr_model.write().overwrite().option('compression', 'lz4').save(model_path)