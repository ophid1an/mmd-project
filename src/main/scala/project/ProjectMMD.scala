package project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import project.customer.{Customer, Spending}
import project.product.Product

object ProjectMMD {

  def main(args: Array[String]): Unit = {
    // Create spark session
    val spark = SparkSession
      .builder()
      .appName("ProjectMMD")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext // Spark context

    // Suppress info messages
    sc.setLogLevel("ERROR")

    val testing = true

    val seed = 1
    val rand = new scala.util.Random(seed)

    val (groceriesFilename, productsFilename, customersMaxCard) =
      if (testing) ("groceries-testing.csv", "products-categorized-testing.csv", 3)
      else ("groceries.csv", "products-categorized.csv", 100)

    def getRandomId: Int = rand.nextInt(customersMaxCard)

    // Set number of partitions for RDDs equal to cores number
    //    val numPartitions = spark.sparkContext.defaultParallelism
    //    val numPartitions = 2

    val basketsRDD = sc
      .textFile(groceriesFilename)
      .map(_.trim.split(',')
        .map(_.trim)
      )
      .cache()

    val productsRDD = sc
      .textFile(productsFilename)
      .map(_.trim.split(',')
        .map(_.trim)
      )
      .map(x =>
        (x(0),
          x(1).split('/')
            .map(_.trim)
        )
      )
      .cache()

    def computeCustomers
    : (Map[Int, Customer[Int, String]],
      Map[Int, Customer[Int, String]],
      Map[Int, Customer[Int, String]]) = {
      val productsMap = productsRDD.collect().map(x => (x._1, Product(x._1, x._2))).toMap
      val productsMapB = sc.broadcast(productsMap)

      val assignedCustomersRDD = basketsRDD
        .map(_.map(productsMapB.value.getOrElse(_, Product()).subCl))
        .map(Customer(getRandomId, Spending[String]()) + Spending(_: _*))
        .map(customer => customer.id -> customer)

      val customersRDD = assignedCustomersRDD.reduceByKey(_ ++ _)

      val customers = customersRDD.collect().toMap

      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.spending.vec.values.sum)
      )

      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.spending.cnt)
      )

      val fractionalCustomers = customers.mapValues(c => Customer(c.id, c.spending.fractional))

      val customersCard = customers.size

      assert(customersCard ==
        fractionalCustomers.values.foldLeft(0.0)(_ + _.spending.vec.values.sum)
      )

      val FractionalSpendingsTotal = fractionalCustomers.values
        .map(_.spending).reduce(_ ++ _)

      val adjustedFractionalSpendingsTotal = FractionalSpendingsTotal
        .vec.mapValues(customersCard / _)

      val normalizedFractionalCustomers = fractionalCustomers.mapValues(
        c => Customer(c.id, c.spending * adjustedFractionalSpendingsTotal)
      )

      (customers, fractionalCustomers, normalizedFractionalCustomers)
    }

    val (customers, fractionalCustomers, normalizedFractionalCustomers) =
      computeCustomers

    // Print customers
    println("\n************ Customers ****************\n")
    customers.take(5).foreach(println)
    println("\n******** Fractional Customers *********\n")
    fractionalCustomers.take(5).foreach(println)
    println("\n*** Normalized Fractional Customers ***\n")
    normalizedFractionalCustomers.take(5).foreach(println)

    // Display statistics
    //    displayStats(basketsRDD, productsRDD)

    spark.stop()
  }

  def displayStats(baskets: RDD[Array[String]], products: RDD[(String, Array[String])]): Unit = {
    val basketsCnt = baskets.count()
    val basketsSizes = baskets.map(_.length)
    val productsCnt = products.count()

    println("**********")
    println("Statistics")
    println("**********")

    println("\nBaskets")
    println("------------------------------")
    println("Count: " + basketsCnt)

    println("First 5 baskets: ")
    for (i <- baskets.take(5)) println("\t" + i.mkString(", "))

    println("First 5 baskets sizes: " + basketsSizes.take(5).mkString(", "))
    println("Min size: " + basketsSizes.min())
    println("Max size: " + basketsSizes.max())
    println("Mean size: " + basketsSizes.mean())

    println("\nProducts")
    println("------------------------------")
    println("Count: " + productsCnt)

    println("First 5 products: ")
    for (i <- products.take(5)) println("\t" + i._1 + " -> " + i._2.mkString(", "))
  }

  def invertMap[A, B](inputMap: Map[A, B]): Map[B, List[A]] = {
    inputMap.foldLeft(Map[B, List[A]]()) {
      case (mapAccumulator, (value, key)) =>
        if (mapAccumulator.contains(key)) {
          mapAccumulator.updated(key, mapAccumulator(key) :+ value)
        } else {
          mapAccumulator.updated(key, List(value))
        }
    }
  }
}
