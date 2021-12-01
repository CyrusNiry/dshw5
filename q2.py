'''
Author: Cyrus Yang
Date: 2021-12-02 05:40:23
LastEditTime: 2021-12-02 05:45:56
'''
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as fc
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")

spark = SparkSession.builder.appName("q2").getOrCreate()
film = spark.read.option("header", "true").csv('film.csv')
film_actor = spark.read.option("header", "true").csv('film_actor.csv')
actor = spark.read.option("header", "true").csv('actor.csv')
# a)
# print('## a)')
# film[(film.rating == 'PG') | (film.rating == 'PG-13')
#      ].agg(fc.count('*').alias('count')).show()

# b)
# print('## b)')
# join_df = film.join(film_actor, "film_id").join(actor, "actor_id")
# join_df[join_df.title == "ANONYMOUS HUMAN"][['first_name', 'last_name']].show()

# c)
print('## c)')
actor_id = film_actor.groupBy('actor_id').agg(
    fc.count('*').alias('cnt')).orderBy('cnt', ascending=False).limit(1)
actor.join(actor_id, 'actor_id')[['first_name', 'last_name']].show()

# d)
print('## d)')
film[film.length >= 60].groupBy('rating').agg(fc.round(fc.mean('rental_rate'), 2).alias(
    'avg_rate'), fc.count('*').alias('cnt')).filter('cnt >= 160')[['rating', 'avg_rate']].show()
