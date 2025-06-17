import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, rand

def main():
    spark = SparkSession.builder \
        .appName("parquet-to-kafka-loop-json") \
        .getOrCreate()

    df = spark.read.parquet("s3a://etl-data-transform/transactions_v2_clean.parquet").cache()

    while True:
        batch_df = df.orderBy(rand()).limit(50)
        kafka_df = batch_df.select(to_json(struct([col(c) for c in batch_df.columns])).alias("value"))

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "rc1a-sp0t812fps48sn74.mdb.yandexcloud.net:9091") \
            .option("topic", "dataproc-kafka-topic") \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
            .option("kafka.sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required "
                    "username=\"user1\" "
                    "password=\"password1\";") \
            .save()

        print("Сообщения отправлены")
        time.sleep(1)

    spark.stop()

if __name__ == "__main__":
    main()