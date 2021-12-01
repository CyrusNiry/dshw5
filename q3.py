'''
Author: Cyrus Yang
Date: 2021-12-02 05:53:53
LastEditTime: 2021-12-02 05:57:07
'''

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf
from operator import add
import pyspark.sql.functions as fc
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")

# a)
print('## a)')
films = sc.textFile('film.csv')
count = films.map(lambda x: x.split(',')).filter(
    lambda List: List[10] == '"PG"' or List[10] == '"PG-13"').map(lambda List: (List[10], 1)).count()
print(count)

# b)
print('## b)')
# your code here

# c)
print('## c)')
# your code here

# d)
print('## d)')
# your code here
