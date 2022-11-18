import os
import sys
from operator import add
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    spark = SparkSession.builder.getOrCreate()
    file_path = 'C:\\Users\\PycharmProjects\\sparkTraining\\data\\word.txt'
    lines = spark.read.text('C:\\Users\\PycharmProjects\\sparkTraining\\data\\word.txt') \
                        .rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                    .map(lambda x: (x, 1)) \
                    .reduceByKey(add)
    res = counts.collect()
    for word, count in res:
        print(f'{word}: {count}')
    spark.stop()

if __name__ == '__main__':
    main()
