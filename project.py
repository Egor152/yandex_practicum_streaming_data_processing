from datetime import datetime
from time import sleep
from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC_NAME_OUT = 'Скрыл по просьбе Яндекс Практикума'  
TOPIC_NAME_IN = 'Скрыл по просьбе Яндекс Практикума' 

#Стриминговые данные


kafka_security_options = {
    'kafka.security.protocol': 'Скрыл по просьбе Яндекс Практикума',
    'kafka.sasl.mechanism': 'Скрыл по просьбе Яндекс Практикума',
    'kafka.sasl.jaas.config': 'Скрыл по просьбе Яндекс Практикума'
}

def spark_init(test_name) -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    spark = (
        SparkSession.builder.appName(test_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
    )

    return spark


def show_stream(df):
    df = (df
             .writeStream
             .outputMode("append")
             .format("console")
             .option('kafka.bootstrap.servers', 'Скрыл по просьбе Яндекс Практикума')
             .options(**kafka_security_options)
             .option("topic", TOPIC_NAME_IN)
             .trigger(once=True)
             .option("truncate", False)
             .start())

    return df.awaitTermination()

def read_stream(spark: SparkSession) -> DataFrame:
    
    df = (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', 'Скрыл по просьбе Яндекс Практикума')
          .options(**kafka_security_options)
          .option("subscribe", TOPIC_NAME_IN) 
          .load()
        .withColumn('value', F.col('value').cast(StringType()))
         )
    return df

def transform_stream(df)->DataFrame:
    """
    first_message:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
    "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
    "adv_campaign_content": "first campaign",
    "adv_campaign_owner": "Ivanov Ivan Ivanovich",
    "adv_campaign_owner_contact": "iiivanov@restaurant_id",
    "adv_campaign_datetime_start": 1659203516,
    "adv_campaign_datetime_end": 2659207116,
    "datetime_created": 1659131516}
    """
    schema_for_project = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", TimestampType()),
        StructField("adv_campaign_datetime_end", TimestampType()),
        StructField("datetime_created", TimestampType())
    ])

    #делаю каст в строку, обрезаю лишнее, возвращаю в TimestampType, но все равно таблица пустая после фильтрации
   
    df = (df.withColumn('data_for_project', F.from_json(F.col('value'), schema_for_project))\
            .withColumn("time", F.current_timestamp().cast(StringType()))\
            .withColumn('time_str', F.split(F.col('time'), '\.')[0])\
            .withColumn("current_timestamp_utc", F.col('time_str').cast(TimestampType()))\
            .selectExpr('data_for_project.*', 'current_timestamp_utc')\
            .filter((F.col("data_for_project.adv_campaign_datetime_start") <= F.col('current_timestamp_utc')) 
                     & (F.col("data_for_project.adv_campaign_datetime_end") >= F.col('current_timestamp_utc')))
         )

    return df

spark = spark_init('project_stream')
default_stream = read_stream(spark)

stream_data = transform_stream(default_stream)

#Статичные данные

postgresql_settings = {
    'user': 'Скрыл по просьбе Яндекс Практикума',
    'password': 'Скрыл по просьбе Яндекс Практикума'
}

def read_subscribers_restaurants(spark: SparkSession) -> DataFrame:
    subscribers_restaurants_df = (spark.read
                    .format("jdbc")
                    .option("url", "Скрыл по просьбе Яндекс Практикума")
                    .option("dbtable", "subscribers_restaurants")
                    .option("driver", "org.postgresql.Driver")
                    .options(**postgresql_settings)
                    .load())

    return subscribers_restaurants_df

spark_for_static_data = spark_init('subscribers_restaurants_from_db')

static_data = read_subscribers_restaurants(spark_for_static_data)

logger.info(f"records from  database subscribers_restaurants: {static_data.count()}")

static_data = static_data.selectExpr('client_id', 'restaurant_id as static_restaurant_id')

#джойню и дедуплицирую данные. 
joined_data = stream_data.join(static_data, on=stream_data['restaurant_id']==static_data['static_restaurant_id'],
                              how='inner')\
                           .drop('static_restaurant_id')\
                           .dropDuplicates(['client_id', 'restaurant_id', 'current_timestamp_utc'])


result_df = joined_data.selectExpr('restaurant_id',
                                   'adv_campaign_id',
                                   'adv_campaign_content',
                                   'adv_campaign_owner',
                                   'adv_campaign_owner_contact',
                                   'adv_campaign_datetime_start',
                                   'adv_campaign_datetime_end',
                                   'datetime_created',
                                   'client_id',
                                   'current_timestamp_utc as trigger_datetime_created')



# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka.
    
    logger.info(f"Processing epoch_id: {epoch_id}")
    
    df.persist(StorageLevel.MEMORY_ONLY)
     

    # записываем df в PostgreSQL с полем feedback.
    logger.info(f"количество записей, которые записываются в subscribers_feedback: {df.count()}")
    

    # добавляю пустю строку для фитбэка
    df_for_db = df.withColumn('feedack', F.lit(''))

    
    df_for_db.write \
      .mode("append") \
      .format("jdbc") \
      .option("url", "Скрыл по просьбе Яндекс Практикума") \
      .option('driver', 'org.postgresql.Driver')\
      .option('user', 'Скрыл по просьбе Яндекс Практикума')\
      .option('password', 'Скрыл по просьбе Яндекс Практикума')\
      .option('schema', 'Скрыл по просьбе Яндекс Практикума')\
      .option('dbtable', 'subscribers_feedback')\
      .save()
    
    # создаём df для отправки в Kafka. Сериализация в json.
    df_for_sending = df.select(F.to_json(F.struct('restaurant_id',
                                                         'adv_campaign_id',
                                                         'adv_campaign_content',
                                                         'adv_campaign_owner',
                                                         'adv_campaign_owner_contact',
                                                         'adv_campaign_datetime_start',
                                                         'adv_campaign_datetime_end',
                                                         'datetime_created',
                                                         'client_id',
                                                         'trigger_datetime_created')).alias('value')
                                     )

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    # поменял код записи данных в топик
    df_for_sending\
             .write\
             .format("kafka")\
             .option('kafka.bootstrap.servers', 'Скрыл по просьбе Яндекс Практикума')\
             .options(**kafka_security_options)\
             .option("topic", TOPIC_NAME_OUT)\
             .save()

    
    # очищаем память от df
    df.unpersist()

result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()


