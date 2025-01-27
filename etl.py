import pyspark
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from subprocess import call

#  initialise spark session
spark = (
    SparkSession.builder
                .appName("etl_test")
                .config("spark.jars", "postgresql-42.2.8.jar") 
                .getOrCreate()
)


#  read spark data - Customers
customers = spark.read.csv(
    "../raw_data/customer_dataset.csv",
    header=True, inferSchema=True, multiLine=True, escape = '"'
    )
customers.printSchema()
customers = customers.withColumnRenamed('customer_zip_code_prefix', 'zip_code') 
customers.show(5)


#  read spark data - Geolocation
geolocation = spark.read.csv(
      "../raw_data/geolocation_dataset.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
geolocation = geolocation.withColumnRenamed('geolocation_zip_code_prefix', 'zip_code') 
geolocation.show(5)

#  read spark data - Order_items
orderItems = spark.read.csv(
    "../raw_data/order_items_dataset.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
orderItems.show(5)

#  read spark data - Order Payments
orderPayments= spark.read.csv(
      "../raw_data/order_payments_dataset.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
orderPayments.show(5)

#  read spark data - Order Reviews
orderReviews= spark.read.csv(
      "../raw_data/order_reviews_dataset.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
orderReviews.show(5)


#  read spark data - Orders Dataset
ordersDataset= spark.read.csv(
      "../raw_data/orders_dataset.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
ordersDataset.show(5)

#  read spark data - Product Category
productCategory= spark.read.csv(
      "../raw_data/product_category_name_translation.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
productCategory.show(5)

#  read spark data - Products Dataset
product= spark.read.csv(
      "../raw_data/product_dataset.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
product.show(5)


#  read spark data - Sellers Dataset
sellers= spark.read.csv(
      "../raw_data/sellers_dataset.csv",
    header=True, inferSchema=True,multiLine=True, escape = '"'
    )
sellers.show(5)



#  Silver Layer Raw Table - Example for Customers table only
customers.write.format("jdbc").options(
    url='jdbc:postgresql://localhost/postgres',
    driver='org.postgresql.Driver',
    user='postgres',
    password='postgres1234',
    dbtable='stackoverflow_filtered.results'
).save(mode='append')


#  Gold Layer - Analytic Table
results = customers.join(ordersDataset, customers.customer_id = ordersDataset.customer_id, how='left')
results = results.join(orderItems, results.customer_id = orderItems.customer_id, how='left')
results = results.join(orderReviews, results.order_id = orderReviews.order_id, how ='left')
results = results.join(product, results.product_id = product.product_id, how = 'left')


results.write.format("jdbc").options(
    url='jdbc:postgresql://localhost/postgres',
    driver='org.postgresql.Driver',
    user='postgres',
    password='postgres1234',
    dbtable='stackoverflow_filtered.results'
).save(mode='append')
