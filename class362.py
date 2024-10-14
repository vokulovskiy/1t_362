import random
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType

count_transactions = 1000
start_date = date.today().replace(day=1, month=1).toordinal()
end_date = date.today().toordinal()
random_day = date.fromordinal(random.randint(start_date, end_date))
products = ['макароны','мука','сахар','сливочное масло','растительное масло','уксус','бобы','овсянка','рис','майонез','томатная паста','мясо','капуста','яблоки','лук','картофель']
transactions = [(date.fromordinal(random.randint(start_date, end_date)),
    random.randint(0, 1000),
    random.choice(products),
    random.randint(0, 1000),
    random.randint(1, 10000))
    for _ in range(count_transactions)]

# Создаем объект SparkSession
spark = SparkSession.builder \
    .appName("shop") \
    .getOrCreate()

# Создаем схему для DataFrame transactions
schema = StructType([
    StructField("date", DateType(), False),
    StructField("userID", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("count", IntegerType(), False),
    StructField("price", IntegerType(), False)
])
df = spark.createDataFrame(transactions, schema)
# Показываем содержимое DataFrame
df.show(5)
# Запись в файл
df.write.csv('transactions')
# Останавливаем SparkSession
spark.stop()