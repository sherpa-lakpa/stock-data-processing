from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import os
import pickle
import random
import pandas as pd
from datetime import datetime

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error


args = {
    'owner': 'pipis',
    'start_date': days_ago(1)
}

dag = DAG(dag_id = 'stock_ml_train', default_args=args, schedule_interval=None)

today = datetime.today().strftime('%Y%m%d')
input_path = "/opt/airflow/data"

def verify_raw_data_path_func():
    stocks_output_path = "/opt/airflow/staging/" + today + "/raw_data_processing/stocks"
    etfs_output_path = "/opt/airflow/staging/" + today + "/raw_data_processing/etfs"

    # Check whether the specified path exists or not
    if not os.path.exists(stocks_output_path):
        # Create a new directory because it does not exist
        os.makedirs(stocks_output_path)
        print("The new directory is created!")

    # Check whether the specified path exists or not
    if not os.path.exists(etfs_output_path):
        # Create a new directory because it does not exist
        os.makedirs(etfs_output_path)
        print("The new directory is created!")

def raw_data_processing_func(type):
    # Set the input paths
    output_path = "/opt/airflow/staging/" + today + "/raw_data_processing/" + type

    # Read the CSV files into Pandas dataframes
    symbols_valid_meta = pd.read_csv(os.path.join(input_path, "symbols_valid_meta.csv"))
    symbols_valid_meta = symbols_valid_meta[['Symbol', 'Security Name']]

    columns = ['Symbol', 'Security Name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    files = os.listdir(os.path.join(input_path, type))

    random.seed(42)
    # selecting random 100 files from both stocks and etfs
    files = random.sample(files, 1000)

    for file in files:
        df = pd.read_csv(os.path.join(input_path, type, file))
        symbol = file.replace('.csv', '')
        df['Symbol'] = symbol
        merged_df = df.merge(symbols_valid_meta, on='Symbol', how='left')
        final_df = merged_df[columns]
        # Write the resulting dataset into a structured format such as Parquet
        final_df.to_parquet(os.path.join(output_path, symbol+".parquet"))

def verify_feature_data_path_func():
    stocks_output_path = "/opt/airflow/staging/" + today + "/feature_engineering/stocks"
    etfs_output_path = "/opt/airflow/staging/" + today + "/feature_engineering/etfs"
    
    # Check whether the specified path exists or not
    if not os.path.exists(stocks_output_path):
        # Create a new directory because it does not exist
        os.makedirs(stocks_output_path)
        print("The new directory is created!")
    
    # Check whether the specified path exists or not
    if not os.path.exists(etfs_output_path):
        # Create a new directory because it does not exist
        os.makedirs(etfs_output_path)
        print("The new directory is created!")

def feature_engineering_processing_func(type):
    raw_data_processing_path = "/opt/airflow/staging/" + today + "/raw_data_processing/"
    output_path = "/opt/airflow/staging/" + today + "/feature_engineering/" + type

    files = os.listdir(os.path.join(raw_data_processing_path, type))

    for file in files:
        df = pd.read_parquet(os.path.join(raw_data_processing_path, type, file))
        # Calculate the rolling average of the trading volume (Volume)
        df['vol_moving_avg'] = df['Volume'].rolling(30).mean()

        # Calculate the rolling median of the Adjusted Close (Adj Close)
        df['adj_close_rolling_med'] = df['Adj Close'].rolling(30).median()

        df.to_parquet(os.path.join(output_path, file))

def train_model_func():
    stocks_output_path = "/opt/airflow/staging/" + today + "/feature_engineering/stocks"
    etfs_output_path = "/opt/airflow/staging/" + today + "/feature_engineering/etfs"

    # Create a RandomForestRegressor model
    model = RandomForestRegressor(n_estimators=100, random_state=42)

    X_test_data = []
    y_test_data = []
    for stock in os.listdir(stocks_output_path):
        stock_df = pd.read_parquet(os.path.join(stocks_output_path, stock))
        # Remove rows with NaN values
        stock_df.dropna(inplace=True)
        
        X, y = stock_df[['vol_moving_avg', 'adj_close_rolling_med']], stock_df['Volume']

        if len(X) < 2:
            X_test_data.append(X)
            y_test_data.append(y)
            continue
        # Split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model.fit(X_train, y_train)
        X_test_data.append(X_test)
        y_test_data.append(y_test)

    print("Stock Data train done!")

    for etf in os.listdir(etfs_output_path):
        etf_df = pd.read_parquet(os.path.join(etfs_output_path, etf))
        etf_df = etf_df[['vol_moving_avg', 'adj_close_rolling_med', 'Volume']]
        etf_df.dropna(inplace=True)
        
        X, y = etf_df[['vol_moving_avg', 'adj_close_rolling_med']], etf_df['Volume']

        if len(X) < 2:
            X_test_data.append(X)
            y_test_data.append(y)
            continue

        # Split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model.fit(X_train, y_train)
        X_test_data.append(X_test)
        y_test_data.append(y_test)

    print("ETF Data train done!")

    X_test = pd.concat(X_test_data)
    y_test = pd.concat(y_test_data)

    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    print("Mean Absolute Error and Mean Squared Error", mae, mse)

    with open('staging/predictive_model.pickle', 'wb') as f:
        pickle.dump(model, f)


with dag:
    with TaskGroup("raw_data_processing", tooltip="Tasks for raw_data_processing") as task_group_raw_data_processing:
        verify_raw_data_path = PythonOperator(task_id="verify_raw_data_path", python_callable = verify_raw_data_path_func)

        etfs_files = os.listdir(os.path.join(input_path, "etfs"))
        stocks_files = os.listdir(os.path.join(input_path, "stocks"))

        columns = ['Symbol', 'Security Name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        stock_data_processing = PythonOperator(
            task_id=f"stock_data_processing", 
            python_callable = raw_data_processing_func,
            op_kwargs={'type': 'stocks'}
        )

        verify_raw_data_path >> stock_data_processing
        
        etf_data_processing = PythonOperator(
            task_id=f"etf_data_processing", 
            python_callable = raw_data_processing_func,
            op_kwargs={'type': 'etfs'}
        )
        
        verify_raw_data_path >> etf_data_processing

    with TaskGroup("feature_engineering", tooltip="Tasks for feature_engineering") as task_group_feature_engineering:
        task_verify_feature_data_path = PythonOperator(
            task_id='verify_feature_data_path',
            python_callable = verify_feature_data_path_func
        )

        stock_feature_engineering = PythonOperator(
            task_id='stock_feature_engineering',
            python_callable = feature_engineering_processing_func,
            op_kwargs={'type': 'stocks'}
        )

        etf_feature_engineering = PythonOperator(
            task_id='etf_feature_engineering',
            python_callable = feature_engineering_processing_func,
            op_kwargs={'type': 'etfs'}
        )

        task_verify_feature_data_path >> [stock_feature_engineering, etf_feature_engineering]

    train_model = PythonOperator(
        task_id='train_model',
        python_callable = train_model_func
    )

    task_group_raw_data_processing >> task_group_feature_engineering >> train_model
