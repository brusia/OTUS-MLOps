from pyspark.sql import SparkSession


def main():
    SparkSubmitOperator
    spark = SparkSession.builder.appName("SamplePySparkJob").getOrCreate()

    # Создаем DataFrame
    data = [("Alice", 30), ("Bob", 25), ("Cathy", 35)]
    df = spark.createDataFrame(data, ["name", "age"])

    # Выводим DataFrame
    df.show()

    # Можно добавить любую обработку
    # Например, фильтрация
    df_filtered = df.filter(df.age > 30)
    df_filtered.show()

    spark.stop()

if __name__ == "__main__":
    main()