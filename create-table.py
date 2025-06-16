from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder.appName("Parquet ETL with Logging to S3").getOrCreate()



source_path = "s3a://data-source717/transactions_v2.csv"
target_path = "s3a://data-ready717/transactions_v2_ready.parquet"

try:
    print(f"Читаем: {source_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

    print("Исходные данные:")
    df.printSchema()

    # Приведение типов,  форматирование даты, расчет нового поля
    df = df.withColumn("actual_amount_paid", col("actual_amount_paid").cast(IntegerType())) \
           .withColumn("is_auto_renew", col("is_auto_renew").cast(BooleanType())) \
           .withColumn("is_cancel", col("is_cancel").cast(BooleanType())) \
           .withColumn("membership_expire_date", to_date(col("membership_expire_date").cast("string"), "yyyyMMdd")) \
           .withColumn("msno", col("msno").cast(StringType())) \
           .withColumn("payment_method_id", col("payment_method_id").cast(IntegerType())) \
           .withColumn("payment_plan_days", col("payment_plan_days").cast(IntegerType())) \
           .withColumn("plan_list_price", col("plan_list_price").cast(IntegerType())) \
           .withColumn("transaction_date", to_date(col("transaction_date").cast("string"),  "yyyyMMdd")) \
           .withColumn("is_long_term_plan", when(col("payment_plan_days") > 30, True).otherwise(False))

    print("Преобразованные данные:")
    df.printSchema()

    # Удаление строк с пропущенными значениями
    df = df.na.drop()

    df.show(5)

    print(f"Запись: {target_path}")
    df.write.mode("overwrite").parquet(target_path)

    print("Данные сохранены в Parquet.")

except AnalysisException as ae:
    print(ae)
except Exception as e:
    print(e)

spark.stop()