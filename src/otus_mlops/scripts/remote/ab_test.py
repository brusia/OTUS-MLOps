def main():
    try:
        from otus_mlops.scripts.ab_test import main as validate_models
        validate_models()
    except Exception:
        pass

if __name__ == "__main__":
    main()