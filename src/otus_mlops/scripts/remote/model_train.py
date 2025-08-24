def main():
    try:
        from otus_mlops.scripts.model_train import main as run_training
        run_training()
    except Exception:
        pass

if __name__ == "__main__":
    main()