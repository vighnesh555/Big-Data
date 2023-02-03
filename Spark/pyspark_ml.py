# -*- coding: utf-8 -*-
"""PySpark_ml.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1S96d3Qd8Cjkz8bIeT0N0RF9c0xlb0BR9
"""

pip install pyspark

import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[4]').appName('ml').getOrCreate()

df = spark.read.csv('diabetes.csv', header =True, inferSchema=True)

df

df.count

df.show(5)

df.summary().show()

pdf = df.toPandas()
pdf

df.groupby('Outcome').count().show()

pdf['Outcome'].value_counts().plot.bar()

import seaborn as sns 
sns.countplot(x = pdf['Outcome'])

round(pdf['Outcome'].value_counts()[0] / len(pdf), 2) * 100

round(pdf['Outcome'].value_counts()[1] / len(pdf), 2) * 100

pdf.isnull().sum()

from pyspark.sql.functions import isnull, when, count, col

#no missing values
df.select([count(when(isnull(c),c)).alias(c) for c in df.columns]).show()

from typing_extensions import Required
# Assemble the features
Required_features = ['Glucose','BloodPressure','BMI','Age']

pdf[Required_features]

# import the vector assembler class
from pyspark.ml.feature import VectorAssembler

# create the object
assembler = VectorAssembler(inputCols=Required_features,outputCol='features')

transformed_data = assembler.transform(df)

transformed_data.show(5)

# split the data in train and test
training_data, test_data = transformed_data.randomSplit([0.75,0.25],seed =0 )

training_data.count()

test_data.count()

training_data.groupby('Outcome').count().show()

test_data.groupby('Outcome').count().show()

from pyspark.ml.classification import DecisionTreeClassifier

dt = DecisionTreeClassifier(labelCol='Outcome',featuresCol='features')

# train the algorith
model =  dt.fit(training_data)

# predictions on unseen data
predictions = model.transform(test_data)

predictions.show(5)

predictions.select('prediction').toPandas()

pred = predictions.select('prediction').toPandas()

actual = test_data.select('Outcome').toPandas()

from sklearn.metrics import accuracy_score, ConfusionMatrixDisplay, classification_report

ConfusionMatrixDisplay.from_predictions(actual, pred)

accuracy_score(actual, pred)

print(classification_report(actual, pred))

# import the evaluation class
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

eval = MulticlassClassificationEvaluator(labelCol='Outcome',metricName='accuracy')

print('Accuracy: ', eval.evaluate(predictions))



