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

    // Spark context
    val sc = spark.sparkContext

    // Suppress info messages
    sc.setLogLevel("ERROR")

    val testing = true

    val seed = 1 // Seed for RNG
    val sampleSize = 5 // Size for samples
    val numPartitions = spark.sparkContext.defaultParallelism // Partitions number for FP-growth
    val minSupport = 0.04 // FP-growth minSupport
    val minConfidence = 0.30 // Association Rules minConfidence

    val (groceriesFilename, productsFilename, customersMaxCard) =
      if (testing) ("groceries-testing.csv", "products-categorized-testing.csv", 3)
      else ("groceries.csv", "products-categorized.csv", 100)

    val maxAbsDeviation = 0.0000001 // Used for assertions

    val rand = new scala.util.Random(seed)

    // Method to assign random IDs to customers
    def getRandomId: Int = rand.nextInt(customersMaxCard)

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

    // Method to display some statistic about transactions and products
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

      // Assign each transaction to a random customer ID
      // TODO: RDD Structure
      // NOTE: Customers cardinality may be less than customersMaxCard
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

      // Assertions:

      // TODO: Is the ... equal to ...
      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum)
      )

      // TODO: Is the ... equal to ...
      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.subClSpending.vec.values.sum)
      )

      // TODO: Is the ... equal to ...
      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.clSpending.cnt)
      )

      // TODO: Is the ... equal to ...
      assert(basketsRDD.map(_.length).sum ==
        customers.values.foldLeft(0.0)(_ + _.subClSpending.cnt)
      )

      val fractionalCustomers = customers.mapValues(_.fractional)

      val customersCard = customers.size

      println("Customers card: " + customersCard)
      println("Folded clSpending: " + fractionalCustomers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum))
      println("Folded subClSpending: " + fractionalCustomers.values.foldLeft(0.0)(_ + _.subClSpending.vec.values.sum))

      // Assertions:

      // TODO: Is the ... roughly equal to ...
      assert(Math.abs(customersCard -
        fractionalCustomers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum)) < maxAbsDeviation
      )

      // TODO: Is the ... roughly equal to ...
      assert(Math.abs(customersCard -
        fractionalCustomers.values.foldLeft(0.0)(_ + _.subClSpending.vec.values.sum)) < maxAbsDeviation
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

      (customers.mapValues(_.idsToStrings(taxonomy)),
        fractionalCustomers.mapValues(_.idsToStrings(taxonomy)),
        normalizedFractionalCustomers.mapValues(_.idsToStrings(taxonomy))
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

    def displayRules(transactions: RDD[Array[String]], minSupport: Double, numPartitions: Int, minConfidence: Double)
    : Unit = {

      //println("First 5 transactions: " + transactions.map(_.length).take(5).mkString(", "))
      //println(s"Number of transactions: ${transactions.count()}")
      val fpg = new FPGrowth()
        .setMinSupport(minSupport)
        .setNumPartitions(numPartitions)
      val model = fpg.run(transactions)

      // println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
      // model.freqItemsets.collect().foreach { itemset =>
      // println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
      // }

      println("AssociationRules")
      model.generateAssociationRules(minConfidence).collect().foreach { rule =>
        println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
          s"${rule.consequent.mkString("[", ",", "]")},${rule.confidence}")
      }

    }

    val productsMap = productsRDD.collect().map(x => (x._1, Product(x._1, x._2))).toMap
    val productsMapB = sc.broadcast(productsMap)

    val classRDD = basketsRDD.map(b => b.map(bItem => (productsMapB.value.getOrElse(bItem, Product()).cl)).distinct).cache()
    val subclassRDD = basketsRDD.map(b => b.map(bItem => (productsMapB.value.getOrElse(bItem, Product()).subCl)).distinct).cache()


    //This is wrong: 85(Fruits-Vegetables), 88(Bread-Bakery), 47(Dairy-Eggs-Cheese), 13(Canned-Goods-Soups)
    //This is wrong:54(Fruits-Vegetables), 4(Dairy-Eggs-Cheese), 34(Beverages)
    //Correct: 85(Fruits-Vegetables), 85(Bread-Bakery), 85(Dairy-Eggs-Cheese), 85(Canned-Goods-Soups)
    //Correct: 88(Fruits-Vegetables), 88(Dairy-Eggs-Cheese), 88(Beverages)
    val testCust = classRDD.map(b => b.map(x => getRandomId.toString + "(" + x + ")")) //Need the same id to each line not different BUT HOW? ? ?
    println("\n****************************************************************\n")
    println("Transactions sample classes: ")
    for (i <- testCust.take(sampleSize)) println("\t" + i.mkString(", "))

    println("Transactions sample size: " + classRDD.map(_.length).take(sampleSize).mkString(", "))

    println("\n--------------------- Rules for classes ----------------------\n")
    displayRules(classRDD, minSupport, numPartitions, minConfidence)
    println("\n--------------------- Rules for subclasses -------------------\n")
    displayRules(subclassRDD, minSupport, numPartitions, minConfidence)

    // Display statistics
    //    displayStats(basketsRDD, productsRDD)

    spark.stop()
  }
}


