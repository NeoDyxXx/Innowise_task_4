#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType


# In[5]:


from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.4.0.jar")\
    .getOrCreate()

df_actor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.actor") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_address = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.address") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_category = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.category") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_city = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.city") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_country = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.country") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_customer = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.customer") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_film = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.film") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_film_actor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.film_actor") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_film_category = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.film_category") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_invenotry = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.inventory") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_language = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.language") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_payment = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.payment") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_rental = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.rental") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_staff = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.staff") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_store = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/test_db") \
    .option("dbtable", "public.store") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()


# # Task 1

# In[15]:


from pyspark.sql import functions as f

sum_film_in_category = df_film.join(df_film_category, df_film.film_id == df_film_category.film_id, 'inner')    .select(df_film.film_id, df_film_category.category_id).alias('sum_film_in_category').groupBy(col('category_id'))    .agg(
        f.count(col('sum_film_in_category.film_id')).alias('count_of_films')
    )

sum_film_in_category = sum_film_in_category    .join(df_category, df_category.category_id == sum_film_in_category.category_id, 'inner')    .select(df_category.name, sum_film_in_category.count_of_films).sort(col('count_of_films').asc())
sum_film_in_category.write.format('com.databricks.spark.csv').save('task_1_pyspark')
sum_film_in_category.show()


# # Task 2

# In[16]:


rental_table_from_actor = df_rental.join(df_invenotry, df_rental.inventory_id == df_invenotry.inventory_id, 'inner')    .join(df_film_actor, df_film_actor.film_id == df_invenotry.film_id, 'inner')    .select(df_film_actor.actor_id, df_rental.return_date, df_rental.rental_date)

rental_table_from_actor = rental_table_from_actor    .withColumn('diff_time_in_hour', (col('return_date').cast('long') - col('rental_date').cast('long'))/3600 )    .select('actor_id', 'diff_time_in_hour')

rental_table_from_actor = rental_table_from_actor.alias('rental_table_from_actor').groupBy(col('actor_id')).agg(
        f.sum(col('diff_time_in_hour')).alias('sum_of_diff_time')
    ).sort(col('sum_of_diff_time').desc()).limit(10)\
    .join(df_actor, df_actor.actor_id == rental_table_from_actor.actor_id, 'inner')\
    .select(df_actor.actor_id, df_actor.first_name, df_actor.last_name, col('sum_of_diff_time'))\
    .sort(col('sum_of_diff_time').asc())
rental_table_from_actor.write.format('com.databricks.spark.csv').save('task_2_pyspark')
rental_table_from_actor.show()


# # Task 3

# In[20]:


result_table = df_payment.join(df_store, df_store.manager_staff_id == df_payment.staff_id, 'inner')    .join(df_invenotry, df_invenotry.store_id == df_store.store_id, 'inner')    .join(df_film_category, df_film_category.film_id == df_invenotry.film_id)    .select(df_film_category.category_id, df_payment.amount).groupBy(col('category_id')).agg(
        f.sum(col('amount')).alias('sum_of_amount')
    )
result_table = result_table.sort(col('sum_of_amount').desc()).limit(1)    .join(df_category, df_category.category_id == result_table.category_id, 'inner')    .select(col('name'), col('sum_of_amount'))
result_table.write.format('com.databricks.spark.csv').save('task_3_pyspark')
result_table.show()


# # Task 4

# In[19]:


result_table = df_film.join(df_invenotry, df_invenotry.film_id == df_film.film_id, 'left')    .select(df_invenotry.inventory_id, df_film.film_id, df_film.title).filter(col('inventory_id').isNull())    .drop(col('inventory_id'))
result_table.show()
result_table.write.format('com.databricks.spark.csv').save('task_4_pyspark')


# # Task 5

# In[22]:


from pyspark.sql import Window

result_table = df_film.join(df_film_category, df_film_category.film_id == df_film.film_id, 'inner')    .join(df_film_actor, df_film_actor.film_id == df_film.film_id)    .select(df_film_actor.film_id, df_film_actor.actor_id, df_film_category.category_id)

result_table = result_table.join(df_category, df_category.category_id == result_table.category_id)    .filter(col('name') == 'Children').select(col('film_id'), col('actor_id'))    .groupBy(col('actor_id')).agg(
        f.count(col('film_id')).alias('count')
    )

w = Window.orderBy(f.desc("count"))
result_table = result_table.select(col('actor_id'), col('count'), rank().over(w).alias('rank'))    .filter(col('rank') < 4).drop('rank').join(df_actor, df_actor.actor_id == result_table.actor_id, 'inner')    .select(df_actor.actor_id, df_actor.first_name, df_actor.last_name, col('count'))

result_table.write.format('com.databricks.spark.csv').save('task_5_pyspark')
result_table.show()


# # Task 6

# In[23]:


table_with_active_and_nonactive = df_customer    .join(df_address, df_address.address_id == df_customer.address_id, 'inner')    .join(df_city, df_city.city_id == df_address.city_id)    .select(df_customer.customer_id, df_customer.activebool, df_city.city_id)

active_customer = table_with_active_and_nonactive.filter(col('activebool') == 1)
non_active_customer = table_with_active_and_nonactive.filter(col('activebool') == 0)

active_customer = active_customer.groupBy(col('city_id').alias('act_city_id')).agg(
        f.count(col('customer_id')).alias('active_count')
    )

non_active_customer = non_active_customer.groupBy(col('city_id').alias('non_act_city_id')).agg(
        f.count(col('customer_id')).alias('non_active_count')
    )

result_table = df_city.join(active_customer, active_customer.act_city_id == df_city.city_id, 'left')    .drop(col('act_city_id')).join(non_active_customer, non_active_customer.non_act_city_id == df_city.city_id, 'left')    .drop(col('non_act_city_id')).select(col('city_id'), col('city'), col('active_count'), col('non_active_count'))    .na.fill(0, subset=['active_count', 'non_active_count']).sort(col('non_active_count').asc())
result_table.write.format('com.databricks.spark.csv').save('task_6_pyspark')
result_table.show()


# # Task 7

# In[26]:


df_rental.columns


# In[30]:


df_rental_with_diff_time = df_rental    .withColumn('diff_time_of_hour', (col('return_date').cast('long') - col('rental_date').cast('long'))/3600)    .select(col('rental_id'), col('inventory_id'), col('customer_id'), col('staff_id'), col('diff_time_of_hour'))
df_rental_with_diff_time


# In[31]:


result_table = df_rental_with_diff_time    .join(df_invenotry, df_invenotry.inventory_id == df_rental_with_diff_time.inventory_id, 'inner')    .join(df_film_category, df_film_category.film_id == df_invenotry.film_id, 'inner')    .join(df_customer, df_customer.customer_id == df_rental_with_diff_time.customer_id, 'inner')    .join(df_address, df_address.address_id == df_customer.address_id, 'inner')    .join(df_city, df_city.city_id == df_address.city_id, 'inner')    .select(df_film_category.category_id, df_city.city_id, df_rental_with_diff_time.diff_time_of_hour)

w = Window.partitionBy('city_id').orderBy(f.desc('sum_of_diff_time_of_hour'))
result_table = result_table.groupBy(col('city_id'), col('category_id')).agg(
        f.sum(col('diff_time_of_hour')).alias('sum_of_diff_time_of_hour')
    ).na.fill(0, subset=['sum_of_diff_time_of_hour']).sort(col('city_id'), col('category_id'))\
    .withColumn('rank', rank().over(w)).filter(col('rank') == 1).drop(col('rank'))

result_table = result_table.join(df_city, df_city.city_id == result_table.city_id, 'inner')    .select(col('city'), col('category_id'), col('sum_of_diff_time_of_hour'))    .join(df_category, df_category.category_id == result_table.category_id)    .select(col('city'), df_category.name, col('sum_of_diff_time_of_hour'))    .withColumn('lower_city', f.lower(col('city')))    .filter((col('lower_city').startswith('a') | col('lower_city').contains('-')))    .drop(col('lower_city')).sort(col('sum_of_diff_time_of_hour').desc())

result_table.repartition(3).write.format('com.databricks.spark.csv').save('task_7_pyspark')
result_table.show()

