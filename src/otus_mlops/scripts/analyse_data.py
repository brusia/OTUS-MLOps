from evidently import DataDefinition, Dataset

from otus_mlops.internals.data.analysers._evidently import EvidentlyDataAnalyser
from otus_mlops.internals.data.analysers._spark_custom import SparkCustomDataAnalyser
# from otus_mlops.internals.data.analysers._tf import TFDataAnalyser
# from otus_mlops.internals.data.loaders._spark_raw import FraudRawDataLoader
from otus_mlops.internals.data.loaders._spark_raw import SparkRawDataLoader
from otus_mlops.internals.interfaces.i_data_loader import LoadingMethod



def analyse():
    print("init analysers")
    spark_data_analyser = SparkCustomDataAnalyser()
    evidently_data_analyser = EvidentlyDataAnalyser()

    print("init loader")
    data_loader = SparkRawDataLoader()

    print("Data Schema")
    schema = DataDefinition(
    numerical_columns=["tranaction_id", "customer_id", "terminal_id", "tx_amont", "tx_time_seconds", "tx_time_days", "tx_fraud", "tx_fraud_scenario"],
    )

    print("load data")
    dataset = data_loader.load(loading_method=LoadingMethod.FullDataset)

    print("analyse")
    res = spark_data_analyser.analyse(dataset)

    print("base statistics")
    print(res.base_stats)

    print("categorical statistics")
    print(res.categorical_stats)

    print("Feature correlations")
    print(res.correlations)

    evidently_data_analyser.analyse(dataset=Dataset.from_any(data=dataset, data_definition=schema))

    print("Finished.")



if __name__ == "__main__":
    analyse()