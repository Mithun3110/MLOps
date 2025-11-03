- Run python train_and_log.py to train Linear Regression on Diabetes and log to local MLflow (./mlruns), note the printed run_id.

- View runs: mlflow ui --backend-store-uri ./mlruns --port 5002 → open http://127.0.0.1:5002 and select LinearRegression-Diabetes-Local.

- (Optional) Serve & test: mlflow models serve -m runs:/<RUN_ID>/model -p 5000 --env-manager local, then POST to http://127.0.0.1:5000/invocations.

# MLFlow Screenshot

![MLFlow Screenshot](image.png)