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

    val dfOrders = df2.withColumn("exploded_orders", explode(col("orders"))).withColumn("exploded_details",explode(col("exploded_orders.orderDetails"))).select("customerId","exploded_orders.orderId","exploded_orders.orderTotal","exploded_details.productId","exploded_details.quantity","exploded_details.unitPrice")

    dfOrders.show(false)


    dfOrders.createOrReplaceTempView("OrderView")

    spark.sql("select customerId,orderId,sum(quantity*unitPrice) from OrderView group by customerId,orderId order by customerId,orderId").show(false)

   /*val dfItems = dfOrders.withColumn("item", explode(col("order.orderDetails")))
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


    df2.createOrReplaceTempView("myView")*/

    //spark.sql("SELECT\n  c.customerId,\n  c.firstName,\n  c.lastName,\n  c.email,\n  o.orderId,\n  o.orderTimestamp,\n  d.productId,\n  d.productName,\n  d.quantity,\n  d.unitPrice,\n  o.orderTotal\nFROM myView c\nLATERAL VIEW explode(c.orders) o AS o\nLATERAL VIEW explode(o.orderDetails) d AS d").show(true)



    spark.stop()
  }
}
