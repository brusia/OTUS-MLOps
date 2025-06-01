from evidently import DataDefinition, Dataset

from internals.data.analysers._evidently import EvidentlyDataAnalyser
from internals.data.analysers._tf import TFDataAnalyser
from internals.data.loaders._fraud_raw import FraudRawDataLoader



def analyse():
    evidently_analyser = EvidentlyDataAnalyser()
    # tf_analyser = TFDataAnalyser()
    data_loader = FraudRawDataLoader()

    schema = DataDefinition(
    numerical_columns=["tranaction_id", "customer_id", "terminal_id", "tx_amont", "tx_time_seconds", "tx_time_days", "tx_fraud", "tx_fraud_scenario"],
    )

    dataset = data_loader.load()
    evidently_analyser.analyse(dataset=Dataset.from_pandas(data=dataset, data_definition=schema))
    # tf_analyser.analyse()


if __name__ == "__main__":
    analyse()