from flask import Flask, request
import pickle
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml import PipelineModel

app = Flask(__name__)
spark = SparkSession.builder.appName("random_reg").config("spark.sql.parquet.compression.codec", "lz4").getOrCreate()

@app.route('/')
def home():
    return "Welcome!"

@app.route('/predict_old', methods=['GET'])
def predict_old():
    # load the trained predictive model
    with open('./staging/predictive_model.pickle', 'rb') as f:
        model = pickle.load(f)
    
    vol_moving_avg = float(request.args.get('vol_moving_avg'))
    adj_close_rolling_med = float(request.args.get('adj_close_rolling_med'))
    
    prediction = model.predict([[vol_moving_avg, adj_close_rolling_med]])
    return str(int(prediction))

@app.route('/predict', methods=['GET'])
def predict():
    # load the trained predictive model
    model = RandomForestRegressionModel.load("/usr/local/spark/staging/regressor.model")
    
    vol_moving_avg = float(request.args.get('vol_moving_avg'))
    adj_close_rolling_med = float(request.args.get('adj_close_rolling_med'))

    new_feature = spark.createDataFrame([(vol_moving_avg, adj_close_rolling_med, 0.0)],["vol_moving_avg", "adj_close_rolling_med", "Volume"])
    vectorAssembler = VectorAssembler(inputCols = ['vol_moving_avg', 'adj_close_rolling_med'], outputCol = 'features')
    vstock_df = vectorAssembler.transform(new_feature)
    vstock_df = vstock_df.select('features', 'Volume')

    predict = model.transform(vstock_df)
    prediction = predict.select('prediction').collect()[0]['prediction']
    # prediction = 123123
    return str(prediction)

if __name__ == '__main__':
    # run the Flask app on port 5000
    app.run(debug=True, host='0.0.0.0', port=5000)
