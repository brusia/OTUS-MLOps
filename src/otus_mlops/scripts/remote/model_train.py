# import mlflow

def main():
    # try:
        # from otus_mlops.scripts.model_train import main as run_train
        # run_train()
    # except Exception:
        # print("Ok")
    
    # print("Ok")
    from mlflow import MlflowClient
    client = MlflowClient(tracking_uri="http://10.0.0.23:5000")

    client.create_experiment("my_exp_1")

if __name__ == "__main__":
    main()