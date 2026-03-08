//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col

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
      .load("./data/customers.avro")

    df2.show(false)
    df2.printSchema()

    df2.show()

    val dfOrders = df2.withColumn("order", explode(col("orders")))

    dfOrders.show(false)

    val dfItems = dfOrders.withColumn("item", explode(col("order.orderDetails")))
    dfItems.show(false)
    dfItems.printSchema()

    val flatDf = dfItems.select(
      col("customerId"),
      col("firstName"),
      col("lastName"),
      col("email"),
      col("order.orderId").alias("orderId"),
      col("order.orderTimestamp").alias("orderTimestamp"),
      col("item.productId").alias("productId"),
      col("item.productName").alias("productName"),
      col("item.quantity").alias("quantity"),
      col("item.unitPrice").alias("unitPrice"),
      col("order.orderTotal").alias("orderTotal")
    )

    flatDf.show(false)


    df2.createOrReplaceTempView("myView")

    spark.sql("SELECT\n  c.customerId,\n  c.firstName,\n  c.lastName,\n  c.email,\n  o.orderId,\n  o.orderTimestamp,\n  d.productId,\n  d.productName,\n  d.quantity,\n  d.unitPrice,\n  o.orderTotal\nFROM myView c\nLATERAL VIEW explode(c.orders) o AS o\nLATERAL VIEW explode(o.orderDetails) d AS d").show(true)



    spark.stop()
  }
}
