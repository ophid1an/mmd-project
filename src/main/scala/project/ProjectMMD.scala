package project

import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object ProjectMMD {

  case class Params(
                     // Seed for the RNG
                     seed: Int = 1,
                     // Customers maximum cardinality
                     customersMaxCard: Int = 100,
                     // FP-growth minSupport
                     minSupport: Double = 0.04,
                     // Association Rules minConfidence
                     minConfidence: Double = 0.30,
                     basketsPath: String = "groceries.csv",
                     productsPath: String = "products-categorized.csv"
                   )

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("mmd-project") {
      head("Project for the Mining of Massive Datasets course.")
      opt[Int]("seed")
        .text(s"seed for the RNG, default: ${defaultParams.seed}")
        .action((x, c) => c.copy(seed = x))
      opt[Int]("customersMaxCard")
        .text(s"customers maximum cardinality, default: ${defaultParams.customersMaxCard}")
        .action((x, c) => c.copy(seed = x))
      opt[Double]("minSupport")
        .text(s"minimal support level, default: ${defaultParams.minSupport}")
        .action((x, c) => c.copy(minSupport = x))
      opt[Double]("minConfidence")
        .text(s"minimal confidence, default: ${defaultParams.minConfidence}")
        .action((x, c) => c.copy(minConfidence = x))
      opt[String]("transactions")
        .text(s"path of csv file containing the transactions, default: ${defaultParams.basketsPath}")
        .action((x, c) => c.copy(basketsPath = x))
      opt[String]("products")
        .text(s"path of csv file containing the products, default: ${defaultParams.productsPath}")
        .action((x, c) => c.copy(productsPath = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
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

    //    val testing = false

    //    val (basketsPath, productsPath, customersMaxCard) =
    //      if (testing) ("groceries-testing.csv", "products-categorized-testing.csv", 3)
    //      else ("groceries.csv", "products-categorized.csv", 100)

    val sampleSize = 5 // Size for samples
    val numPartitions = spark.sparkContext.defaultParallelism // Partitions number for FP-growth
    val maxAbsDeviation = 0.0000001 // Used for assertions

    val rand = new scala.util.Random(params.seed)

    // Method to assign random IDs to customers
    def getRandomId: Int = rand.nextInt(params.customersMaxCard)

    // Method to mine association rules
    def mineRules(transactions: RDD[Array[Int]], minSupport: Double,
                  numPartitions: Int, minConfidence: Double)
    : Array[AssociationRules.Rule[Int]] = {

      val model = new FPGrowth()
        .setMinSupport(minSupport)
        .setNumPartitions(numPartitions)
        .run(transactions)

      model.generateAssociationRules(minConfidence).collect()
    }

    // Method to display some statistics about transactions and products
    def displayStats(baskets: RDD[Array[String]],
                     products: RDD[(String, Array[String])]): Unit = {
      val basketsCnt = baskets.count()
      val basketsSizes = baskets.map(_.length)
      val productsCnt = products.count()

      println("\n**********")
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

    val basketsRDD = sc
      .textFile(params.basketsPath)
      .map(_.trim.split(',')
        .map(_.trim)
      )

    val productsRDD = sc
      .textFile(params.productsPath)
      .map(_.trim.split(',')
        .map(_.trim)
      )
      .map(x =>
        (x(0),
          x(1).split('/')
            .map(_.trim)
        )
      )

    val taxonomy = productsRDD.collect()
      .foldLeft(Taxonomy())((acc, i) => acc ++ Taxonomy(i._1, i._2))

    // Broadcast variables
    val productsB = sc.broadcast(taxonomy.products)
    val productsToSubClassesB = sc.broadcast(taxonomy.productsToSubClasses)
    val subClassesToClassesB = sc.broadcast(taxonomy.subClassesToClasses)

    val transformedBasketsRDD = basketsRDD
      .map(b => b.map(bItem => productsToSubClassesB.value.getOrElse(productsB.value.getOrElse(bItem, -1), -1)))
      .cache()

    // Assign each transaction to a random customer ID
    // TODO: RDD Structure
    // NOTE: Customers cardinality may be less than customersMaxCard
    val assignedBasketsRDD = transformedBasketsRDD
      .map(b => {
        val clArr = b.map(subClassesToClassesB.value.getOrElse(_, -1))
        Customer(getRandomId,
          Spending[Int]() ++ Spending(clArr: _*),
          Spending[Int]() ++ Spending(b: _*))
      })
      .map(customer => customer.id -> customer)

    val customersRDD = assignedBasketsRDD.reduceByKey(_ ++ _)

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

    // Print customers sample
    println("\n************ Customers sample ****************\n")
    customers
      .mapValues(_.idsToStrings(taxonomy))
      .take(sampleSize).foreach(println)
    println("\n******** Fractional Customers sample *********\n")
    fractionalCustomers
      .mapValues(_.idsToStrings(taxonomy))
      .take(sampleSize).foreach(println)
    println("\n*** Normalized Fractional Customers sample ***\n")
    normalizedFractionalCustomers
      .mapValues(_.idsToStrings(taxonomy))
      .take(sampleSize).foreach(println)

    // Distinct RDDs
    val subClassesRDD = transformedBasketsRDD
      .map(_.distinct)
      .cache()

    val classesRDD = subClassesRDD
      .map(b => b.map(subCl => subClassesToClassesB.value.getOrElse(subCl, -1)).distinct)

    val classesRules = mineRules(classesRDD, params.minSupport, numPartitions, params.minConfidence)
    val subClassesRules = mineRules(subClassesRDD, params.minSupport, numPartitions, params.minConfidence)

    println("\n****************************************************************\n")
    println("Transactions classes sample: ")
    val sample = classesRDD.map(b => b.map(cl => taxonomy.idsToClasses.getOrElse(cl, "NADA")))
      .take(sampleSize)
    sample.foreach(e => println(e.mkString(", ")))
    println("Transactions classes sample size: " + sample.map(_.length).mkString(", "))

    println("\n--------------------- Rules for classes ----------------------\n")

    classesRules.foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
        s"${rule.consequent.mkString("[", ",", "]")},${rule.confidence}")
    }
    println("\n--------------------- Rules for subclasses -------------------\n")

    subClassesRules.foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
        s"${rule.consequent.mkString("[", ",", "]")},${rule.confidence}")
    }

    //Display statistics
    //    displayStats(basketsRDD, productsRDD)

    spark.stop()
  }
}


