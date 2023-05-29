# Stock Data Processing
This project creates a data pipeline for stock market data. The pipeline has the following stages:
1. Verify whether the raw data processing paths exist, and create them if necessary.
2. Read the CSV files into dataframes, merge the data with a metadata file, and write the resulting dataset into a structured format such as Parquet.
3. Verify whether the feature engineering paths exist, and create them if necessary.
4. Calculate the rolling average of the trading volume and the rolling median of the Adjusted Close, and write the resulting dataset into a staging Parquet file.
5. Train a RandomForestRegressor model on the feature-engineered data, and calculate the model's performance metrics.

## DAG components
![DAG](/docs/dagv0_1.png)
- verify_raw_data_path_task: Verify whether the raw data processing paths exist, and create them if necessary.
- raw_data_processing_task_group: Read the CSV files into Pandas dataframes, merge the data with a metadata file, and write the resulting dataset into a structured format such as Parquet.
- verify_feature_data_path_task: Verify whether the feature engineering paths exist, and create them if necessary.
- feature_engineering_task_group: Calculate the rolling average of the trading volume and the rolling median of the Adjusted Close, and write the resulting dataset into a structured format such as Parquet.
- train_model_task: Train a RandomForestRegressor model on the feature-engineered data, and calculate the model's performance metrics.

## Architecture components
![Architecture](/docs/riskthinking.drawio.png)

## How to use
Before running this program, you will need to follow below steps:
### Step 1: Clone repo
Please clone or download this repository to your local machine.
### Step 2: Install Docker and Docker Compose
You can follow this [link](https://docs.docker.com/desktop/).
### Step 3: Create two additional directories in repo
```sh
mkdir ./data ./staging
```
### Step 4: Download the data from Kaggle in ./data directory
Download the ETF and stock datasets from the primary dataset available at [Kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset) and move to `data` directory.
### Step 5: Build the Images
```sh
docker build -f Dockerfile.Spark . -t airflow-spark
```

```sh
docker build -f Dockerfile.Flask . -t flask-app
```
### Step 5: Initialise the Airflow Database
```sh
docker-compose up airflow-init
```
### Step 6: Start Airflow services
```sh
docker-compose up
```
## Access necessary links
### Airflow: 

[localhost:8080](localhost:8080) 

By default, username and password will be <strong>airflow</strong> and hit ‘Sign in’.

Create a new spark connection with detail as shown in the image.
![Connection](/docs/connection.png)

### Spark Master:
[http://localhost:8090/](http://localhost:8090/)
![Spark UI](/docs/sparkui.png)

### Jupyter Notebook:
[http://127.0.0.1:8888](http://127.0.0.1:8888)

For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:

```sh
docker logs -f de-rt-jupyter-spark-1
```

### Model Serving API:
[http://127.0.0.1:8008/](http://127.0.0.1:8008/)
You will get 'Welcome!' response on default link. You can use /predict API endpoint which takes two values, vol_moving_avg and adj_close_rolling_med, and return with an integer value that represents the trading volume.
```
http://127.0.0.1:8008/predict?vol_moving_avg=12345&adj_close_rolling_med=55
```


## Author
This data pipeline was created by [Lakpa Sherpa](https://slakpa.com.np).