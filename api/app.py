from flask import Flask, request
import pickle

app = Flask(__name__)

@app.route('/predict', methods=['GET'])
def predict():
    # load the trained predictive model
    with open('./staging/predictive_model.pickle', 'rb') as f:
        model = pickle.load(f)
    
    vol_moving_avg = float(request.args.get('vol_moving_avg'))
    adj_close_rolling_med = float(request.args.get('adj_close_rolling_med'))
    
    prediction = model.predict([[vol_moving_avg, adj_close_rolling_med]])
    return str(int(prediction))

if __name__ == '__main__':
    # run the Flask app on port 5000
    app.run(debug=True, port=5000)
