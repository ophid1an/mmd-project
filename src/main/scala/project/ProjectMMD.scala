package project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.fpm.AssociationRules
import collection.breakOut
import project.customer.{Customer, Spending}
import project.product.{Product, Taxonomy}


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
    val sampleSize = 5

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

      println("Baskets sample: ")
      for (i <- baskets.take(sampleSize)) println("\t" + i.mkString(", "))

      println("Baskets sample sizes: " + basketsSizes.take(sampleSize).mkString(", "))
      println("Min size: " + basketsSizes.min())
      println("Max size: " + basketsSizes.max())
      println("Mean size: " + basketsSizes.mean())

      println("\nProducts")
      println("------------------------------")
      println("Count: " + productsCnt)

      println("Products sample: ")
      for (i <- products.take(sampleSize)) println("\t" + i._1 + " -> " + i._2.mkString(", "))
    }

    def computeCustomers
    : (Map[Int, Customer[Int, String]],
      Map[Int, Customer[Int, String]],
      Map[Int, Customer[Int, String]]) = {
      val taxonomy = productsRDD.collect().foldLeft(Taxonomy())((acc, i) => acc ++ Taxonomy(i._1, i._2))
      val productsToSubClassesB = sc.broadcast(taxonomy.productsToSubClasses)
      val productsB = sc.broadcast(taxonomy.products)
      val subClassesToClassesB = sc.broadcast(taxonomy.subClassesToClasses)

      val transformedBasketsRDD = basketsRDD
        .map(b => b.map(bItem => productsToSubClassesB.value.getOrElse(productsB.value.getOrElse(bItem, -1), -1)))

      val assignedCustomersRDD = transformedBasketsRDD
        .map(b => {
          val clArr = b.map(subClassesToClassesB.value.getOrElse(_, -1))
          Customer(getRandomId,
            Spending[Int]() ++ Spending(clArr: _*),
            Spending[Int]() ++ Spending(b: _*))
        })
        .map(customer => customer.id -> customer)

      val customersRDD = assignedCustomersRDD.reduceByKey(_ ++ _)

      val customers = customersRDD.collect().toMap

      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum)
      )

      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.subClSpending.vec.values.sum)
      )

      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.clSpending.cnt)
      )

      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.subClSpending.cnt)
      )

      val fractionalCustomers = customers.mapValues(_.fractional)

      val customersCard = customers.size

      println("Customers card: " + customersCard)
      println("Folded clSpending: " + fractionalCustomers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum))
      println("Folded subClSpending: " + fractionalCustomers.values.foldLeft(0.0)(_ + _.subClSpending.vec.values.sum))

      assert(Math.abs(customersCard -
        fractionalCustomers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum)) < 0.00001
      )

      assert(Math.abs(customersCard -
        fractionalCustomers.values.foldLeft(0.0)(_ + _.subClSpending.vec.values.sum)) < 0.00001
      )


      val fractionalClSpendingsTotal = fractionalCustomers.values
        .map(_.clSpending).reduce(_ ++ _)

      val fractionalSubClSpendingsTotal = fractionalCustomers.values
        .map(_.subClSpending).reduce(_ ++ _)

      val adjustedFractionalClSpendingsTotal = fractionalClSpendingsTotal.vec
        .mapValues(customersCard / _)

      val adjustedFractionalSubClSpendingsTotal = fractionalSubClSpendingsTotal.vec
        .mapValues(customersCard / _)

      val normalizedFractionalCustomers = fractionalCustomers.mapValues(
        c => Customer(c.id,
          c.clSpending * adjustedFractionalClSpendingsTotal,
          c.subClSpending * adjustedFractionalSubClSpendingsTotal)
      )

      //      (customers, fractionalCustomers, fractionalCustomers)
      (
        customers.mapValues(c => Customer(c.id,
          Spending(c.clSpending.vec.map {
            case (k, v) => taxonomy.idsToClasses.getOrElse(k, "") -> v
          }),
          Spending(c.subClSpending.vec.map {
            case (k, v) => taxonomy.idsToSubClasses.getOrElse(k, "") -> v
          }))),
        fractionalCustomers.mapValues(c => Customer(c.id,
          Spending(c.clSpending.vec.map {
            case (k, v) => taxonomy.idsToClasses.getOrElse(k, "") -> v
          }),
          Spending(c.subClSpending.vec.map {
            case (k, v) => taxonomy.idsToSubClasses.getOrElse(k, "") -> v
          }))),
        normalizedFractionalCustomers.mapValues(c => Customer(c.id,
          Spending(c.clSpending.vec.map {
            case (k, v) => taxonomy.idsToClasses.getOrElse(k, "") -> v
          }),
          Spending(c.subClSpending.vec.map {
            case (k, v) => taxonomy.idsToSubClasses.getOrElse(k, "") -> v
          })))
      )
    }

    val (customers, fractionalCustomers, normalizedFractionalCustomers) =
      computeCustomers

    // Print customers sample
    println("\n************ Customers sample ****************\n")
    customers.take(sampleSize).foreach(println)
    println("\n******** Fractional Customers sample *********\n")
    fractionalCustomers.take(sampleSize).foreach(println)
    println("\n*** Normalized Fractional Customers sample ***\n")
    normalizedFractionalCustomers.take(sampleSize).foreach(println)

    // Display statistics
    //    displayStats(basketsRDD, productsRDD)

    spark.stop()
  }
}


