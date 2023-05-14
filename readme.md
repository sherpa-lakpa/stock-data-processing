# Stock Data Processing
This project creates a data pipeline for stock market data. The pipeline has the following stages:
1. Verify whether the raw data processing paths exist, and create them if necessary.
2. Read the CSV files into Pandas dataframes, merge the data with a metadata file, and write the resulting dataset into a structured format such as Parquet.
3. Verify whether the feature engineering paths exist, and create them if necessary.
4. Calculate the rolling average of the trading volume and the rolling median of the Adjusted Close, and write the resulting dataset into a structured format such as Parquet.
5. Train a RandomForestRegressor model on the feature-engineered data, and calculate the model's performance metrics.

The DAG consists of five tasks:
![DAG](/docs/dagv0_1.png)
- verify_raw_data_path_task: Verify whether the raw data processing paths exist, and create them if necessary.
- raw_data_processing_task_group: Read the CSV files into Pandas dataframes, merge the data with a metadata file, and write the resulting dataset into a structured format such as Parquet.
- verify_feature_data_path_task: Verify whether the feature engineering paths exist, and create them if necessary.
- feature_engineering_task_group: Calculate the rolling average of the trading volume and the rolling median of the Adjusted Close, and write the resulting dataset into a structured format such as Parquet.
- train_model_task: Train a RandomForestRegressor model on the feature-engineered data, and calculate the model's performance metrics.

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
Download the ETF and stock datasets from the primary dataset available at [Kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset)
### Step 5: Initialise the Airflow Database
```sh
docker-compose up airflow-init
```
### Step 6: Start Airflow services
```sh
docker-compose up
```

## Author
This program was created by [Lakpa Sherpa](https://slakpa.com.np).