//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkApp")
      .master("local[*]") // run locally using all cores
      .getOrCreate()

    println("Spark version: " + spark.version)

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./data/sample.csv")

    df.show()

    val df2 = spark.read
      .format("avro")
      .load("./data/BlueSkyProfiles.avro")

    df2.show(false)
    df2.printSchema()

    df2.show()


    spark.stop()
  }
}
