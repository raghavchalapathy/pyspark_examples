import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/osboxes/.cache/spark/spark-2.0.1-bin-hadoop2.7"
# Append pyspark to Python Path
sys.path.append("/home/osboxes/.cache/spark/spark-2.0.1-bin-hadoop2.7/python")

os.environ["PYSPARK_SUBMIT_ARGS"] = (
  "--driver-memory 12g --packages com.databricks:spark-csv_2.11:1.3.0,saurfang:spark-sas7bdat:1.1.5-s_2.11  pyspark-shell"

)

LINE_LENGTH = 50
def print_horizontal():
    """
    Simple method to print horizontal line
    :return: None
    """
    for i in range(LINE_LENGTH):
        sys.stdout.write('-')
    print("")


try:
    from pyspark import SparkContext
    from pyspark import SQLContext
    import pandas as pd

    print_horizontal()
    print ("Successfully imported Spark Modules -- `SparkContext, SQLContext`")
    print_horizontal()
    SparkContext.setSystemProperty('spark.driver.memory', '20g')
    SparkContext.setSystemProperty('spark.executor.memory', '20g')

    sc = SparkContext("local")
    print_horizontal()
    print sc._conf.getAll()
    print_horizontal()
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/osboxes/PycharmProjects/demo_pyspark/cars.csv')
    # df.select('year', 'model').write.format('com.databricks.spark.csv').save('newcars.csv')
    df = sqlContext.read.format("com.github.saurfang.sas.spark").load("/home/osboxes/PycharmProjects/demo_pyspark/airline.sas7bdat")
    print df.head(5)
    pd_df = df.toPandas()
    print_horizontal()
    print " The converting the spark df to pandas df"
    print pd_df.head()
    print_horizontal()
    print type(pd_df)


except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)