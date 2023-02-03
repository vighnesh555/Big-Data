# -*- coding: utf-8 -*-
"""PySpark.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1YmoHYvETDVz04jtz1rlJ-oecu6CbyCp8
"""

pip install pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[4]")\
        .appName('MITU_Skillologies')\
        .getOrCreate()

# spark context object
sc = spark.sparkContext

#create RDD from list
rdd = sc.parallelize([1,2,3,4,5,6,7,8])
print(rdd.collect())
print('Number of partitions', rdd.getNumPartitions())

rdd2 = rdd.repartition(2)
print('Number of partitions', rdd2.getNumPartitions())

rdd.saveAsTextFile('data')

rdd2.saveAsTextFile('data1')

rdd = sc.textFile('fruits.txt')

rdd.collect()

import os
os.getcwd()

rdd1 = sc.textFile('fruits.txt,fruits1.txt')
rdd1.collect()

rdd2 = rdd1.flatMap(lambda x:x.split())
rdd2.collect()

rdd3 = rdd2.map(lambda x: (x,1))
rdd3.collect()

rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
rdd4.collect()

rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
rdd5.collect()

rdd5 = rdd4.map(lambda x: (x[0],x[1])).sortByKey()
rdd5.collect()

rdd4 = rdd3.filter(lambda x: x[0].startswith('B'))
print(rdd4.collect())

text_file = sc.textFile("fruits.txt")
counts = text_file.flatMap(lambda line: line.split(" "))\
          .map(lambda word: (word, 1))\
          .reduceByKey(lambda x, y: x+y)

output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))

text_file = sc.textFile("fruits.txt")
counts = text_file.flatMap(lambda line: line.split(" "))\
          .map(lambda word: (word, 1))\
          .reduceByKey(lambda x, y: x+y)

text_file = sc.textFile("ages.txt")
counts = text_file.flatMap(lambda line: line.split())\
          .filter(lambda x : x.isdigit())\
          .map(lambda word:(1,int(word)))

def average(rdd):
  add = 0; count = 0
  for x, y in rdd.collect():
    add += y
    count +=1
    return(sc.parallelize(['Average is: ',add /count]))

val = average(counts)
val.collect()

rdd = sc.parallelize([1,2,3,4,5,6,7,8])
rdd.collect()

print("Count : " + str(rdd.count()))

firstRec = rdd.first()
print("First Record : "+str(firstRec))

print("Maximum: ",rdd.max())

print("Take 3: ",rdd.take(3))

rdd.sum()

rdd.mean()

df = spark.read.option('header', True).csv('student.csv')

df

df.printSchema()

df.head(4)

df1 = spark.read.csv('student3.tsv')

df1.head(4)

df1 = spark.read.option('delimiter','\t').option('header',True).csv('student3.tsv')

df1.head(4)

df1.tail(4)

df1 = spark.read.options(delimiter='\t',header=True).csv('student3.tsv')

df1.head(4)

df1 = spark.read.options(inferSchema=True,header=True).csv('student.csv')

df1.printSchema()

df

from pyspark.sql.types import *

schema = StructType()\
      .add("roll",IntegerType(),True)\
      .add("name",StringType(),True)\
      .add("class",StringType(),True)\
      .add("marks",DoubleType(),True)\
      .add("age",IntegerType(),True)\

df_s=spark.read.format("csv")\
    .option("header", True)\
    .schema(schema) \
    .load("student.csv")

df_s.head(4)

df_s.write.options(header=True).csv('newfile')

df2 = spark.read.json('students.json')

df2.head(4)

type(df2)

df1.show()

df2.show()

df1.select('name').show()

df1.select(['name','marks']).show()

df.filter(df['marks']>60).show()

df.filter((df['class']=='BE')&(df['marks']>60)).show()

df1.groupBy('class').count().show()

df1.groupBy('class').max().show()

df1.select(['class','marks']).groupBy('class').max().show()



x = df.toDF('roll','name','class','marks','age')

x.show(5)

a = [1,2,3]
b =a

b.append(4)

a

id(a)

id(x), id(df)

newdf = df.toDF('1','2','3','4','5')
newdf.show()

# count total number of rows
df.count()

# print the names of columns
df.columns

newdf.columns

df.take(6)

# drop the unwanted columns
df.drop('class').show(5)

# drop the unwanted columns
df.drop('class','marks').show(5)

pdf =  df1.toPandas()
pdf

gender = ['M','M','M','M','F','F','M','F','M','M']

pdf['gender'] = gender
pdf

df1.select('marks').show()

pdf['marks'] = pdf['marks'] + 0.5
pdf

pdf['name'].str.upper()
pdf

pdf['marks'].plot()

pdf.plot()

pdf.plot.bar()

pdf['marks'].plot.hist()

pdf['marks'].plot.box()

pdf['marks'].plot.pie()

# Check the data in vertical format
df.show(n=3, vertical=True)

# Check the data in sorted format
df.orderBy('marks', ascending=False).show(5)

pdf.sort_values(by='marks',ascending=False).head()

df1.describe().show()

df1.describe('marks','age').show()

df1.agg({'marks':'mean'}).show()

df.summary().show()

df.summary('mean','50%').show()


