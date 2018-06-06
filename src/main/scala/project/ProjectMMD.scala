package project

import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vector

object ProjectMMD {

  // parameters configuration
  case class Params(
     // Target customer for whom to provide recommendations
     target: Int = 0,

     // Seed for the RNG
     seed: Int = 1,

     // Customers maximum cardinality
     customersMaxCard: Int = 100,

     // Number of clusters
     clustersNum: Int = 6,

     // FP-growth minSupport
     minSupport: Double = 0.04,

     // Association Rules minConfidence
     minConfidence: Double = 0.30,

     // relative path to csv data
     basketsPath: String = "groceries.csv",
     productsPath: String = "products-categorized.csv"
  )

  /**
    * Main method
    * @param args
    */
  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("mmd-project") {
      head("Project for the Mining of Massive Datasets course.")
      opt[Int]("target")
        .text(s"Target customer ID for whom to provide recommendations, default: ${defaultParams.target}")
        .action((x, c) => c.copy(target = x))
      opt[Int]("seed")
        .text(s"seed for the RNG, default: ${defaultParams.seed}")
        .action((x, c) => c.copy(seed = x))
      opt[Int]("customers")
        .text(s"customers maximum cardinality, default: ${defaultParams.customersMaxCard}")
        .action((x, c) => c.copy(customersMaxCard = x))
      opt[Int]("clusters")
        .text(s"number of customer clusters, default: ${defaultParams.clustersNum}")
        .action((x, c) => c.copy(clustersNum = x))
      opt[Double]("support")
        .text(s"minimal support level, default: ${defaultParams.minSupport}")
        .action((x, c) => c.copy(minSupport = x))
      opt[Double]("confidence")
        .text(s"minimal confidence, default: ${defaultParams.minConfidence}")
        .action((x, c) => c.copy(minConfidence = x))
      opt[String]("transactions")
        .text(s"path of csv file containing the transactions, default: ${defaultParams.basketsPath}")
        .action((x, c) => c.copy(basketsPath = x))
      opt[String]("products")
        .text(s"path of csv file containing the products, default: ${defaultParams.productsPath}")
        .action((x, c) => c.copy(productsPath = x))
      help("help").text("prints this usage text")
    }

    // use valid defaultParams or exit
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  /**
    * Runs most of the business logic. Invoked by main
    * @param params
    */
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

    val sampleSize = 5 // Size for samples
    val iterationsNum = 20 // Iterations for KMeans
    val numPartitions = spark.sparkContext.defaultParallelism // Partitions number for FP-growth
    val maxAbsDeviation = 0.0000001 // Used for assertions

    // initialize a random generator
    val rand = new scala.util.Random(params.seed)

    // Method to assign random IDs to customers
    def getRandomId: Int = rand.nextInt(params.customersMaxCard)

    // load the baskets in RDD
    val basketsRDD = sc
      .textFile(params.basketsPath)
      .map(_.trim.split(',')
        .map(_.trim)
      )

    // load the products in RDD
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
    val classesToSubClassesB = sc.broadcast(taxonomy.classesToSubClasses)

    val transformedBasketsRDD = basketsRDD
      .map(b => b
        .map(bItem => productsToSubClassesB.value.getOrElse(productsB.value.getOrElse(bItem, -1), -1)))
      .cache()

    // Assign each transaction to a random customer ID
    // NOTE: Customers cardinality may be less than customersMaxCard
    val assignedBasketsRDD = transformedBasketsRDD
      .map(b => {
        val clArr = b.map(subClassesToClassesB.value.getOrElse(_, -1))
        Customer(Spending[Int]() ++ Spending(clArr: _*),
          Spending[Int]() ++ Spending(b: _*))
      })
      .map(customer => getRandomId -> customer)

    val customersRDD = assignedBasketsRDD.reduceByKey(_ ++ _)

    val customers = customersRDD.collect().toMap

    val customersCard = customers.size

    // Assertions:

    // Is the number of customers equal to customersMaxCard?
    assert(customersCard == params.customersMaxCard)

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

    // normalize on customer's total spending
    val fractionalCustomers = customers.mapValues(_.fractional)

//    println("Customers card: " + customersCard)
//    println("Folded clSpending: " + fractionalCustomers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum))
//    println("Folded subClSpending: " + fractionalCustomers.values.foldLeft(0.0)(_ + _.subClSpending.vec.values.sum))

    // More assertions:
    assert(Math.abs(customersCard -
      fractionalCustomers.values.foldLeft(0.0)(_ + _.clSpending.vec.values.sum)) < maxAbsDeviation
    )

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

    // normalize on product's popularity
    val normalizedFractionalCustomers = fractionalCustomers.mapValues(
      c => Customer(c.clSpending * adjustedFractionalClSpendingsTotal,
        c.subClSpending * adjustedFractionalSubClSpendingsTotal)
    )

    // Print customers sample
//    println("\n************ Customers sample ****************\n")
//    customers
//      .mapValues(_.idsToStrings(taxonomy))
//      .take(sampleSize).foreach(println)
//
//    println("\n******** Fractional Customers sample *********\n")
//    fractionalCustomers
//      .mapValues(_.idsToStrings(taxonomy))
//      .take(sampleSize).foreach(println)
//
//    println("\n*** Normalized Fractional Customers sample ***\n")
//    normalizedFractionalCustomers
//      .mapValues(_.idsToStrings(taxonomy))
//      .take(sampleSize).foreach(println)

    /** *******************************
      * ** Association Rules Mining ***
      * *******************************/

    // RDD with distinct subclasses ids for each transaction
    val subClassesRDD = transformedBasketsRDD
      .map(_.distinct)
      .cache()

    // RDD with distinct classes ids for each transaction
    val classesRDD = subClassesRDD
      .map(b => b.map(subCl => subClassesToClassesB.value.getOrElse(subCl, -1)).distinct)
      .cache()

    // Mine association rules on classes level & filter on minSupport, minConfidence, itemset size == 1
    val classesRules = mineRules(classesRDD, params.minSupport, numPartitions, params.minConfidence)
      .filter(_.antecedent.length == 1)

    // Mine association rules on subclass level & filter on minSupport, minConfidence, itemset size == 1
    val subClassesRules = mineRules(subClassesRDD, params.minSupport, numPartitions, params.minConfidence)
      .filter(_.antecedent.length == 1)

    // Products RDD for content-based and collaborative filtering
    val transformedProductsRDD: RDD[(Int, Product[Int])] = productsRDD.map {
      case (k, _) => {

        val prodId = productsB.value.getOrElse(k, -1)
        val prodSubClass = productsToSubClassesB.value.getOrElse(prodId, -1)
        val prodClass = subClassesToClassesB.value.getOrElse(prodSubClass, -1)
        val prodSiblingSubClasses = classesToSubClassesB.value.getOrElse(prodClass, List[Int]()).toSet - prodSubClass
        val prodDirectlyAssociatedSubClasses = subClassesRules.filter(_.antecedent(0) == prodSubClass)
          .flatMap(_.consequent).toSet -- prodSiblingSubClasses
        val prodIndirectlyAssociatedSubClasses = classesRules.filter(_.antecedent(0) == prodClass)
          .flatMap(_.consequent).flatMap(cl => classesToSubClassesB.value.getOrElse(cl, List[Int]()))
          .toSet -- prodDirectlyAssociatedSubClasses

        // compute score for P^(n) vector
        (prodId,
          (Product(Map[Int, Double]())
            + prodIndirectlyAssociatedSubClasses.map(x => x -> .25).toMap // within subclass of associated class
            + prodDirectlyAssociatedSubClasses.map(x => x -> 1.0).toMap // within associated class
            + prodSiblingSubClasses.map(x => x -> 0.5).toMap) // subclass with same class
            + Map(prodSubClass -> 1.0) // within same subclass
        )
      }
    }

//    println("\n\n**** Products Vectors ****\n\n")
//    transformedProductsRDD.collect().foreach(prod => {
//      println(s"Product ID: ${prod._1}\n\tVector: ${prod._2}\n")
//    })


    /** *****************
      * ** Clustering ***
      * *****************/

    val parsedData = sc.parallelize(normalizedFractionalCustomers.toSeq.map {
      case (_, v) => v.clSpending.sparseVec(classesToSubClassesB.value.size)
    })

//    println("\n\n****** Clustering using customers' normalized fractional class spendings ******\n\n")

    // Calculate WSSSE for different values of k
//    Range(1, 21).foreach(clusterSize => {
//      val clusters = findClusters(parsedData, clusterSize, iterationsNum)
//      // Evaluate clustering by computing Within Set Sum of Squared Errors
//      println(s"Cluster size: $clusterSize")
//      println(s"Within Set Sum of Squared Errors = ${clusters.computeCost(parsedData)}\n")
//    })


    //Display statistics
    //displayStats(basketsRDD, productsRDD, sampleSize)

    spark.stop()

  } // run




  def findClusters(data: RDD[Vector], clustersNum: Int, iterationsNum: Int): KMeansModel =
    KMeans.train(data, clustersNum, iterationsNum)

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
                   products: RDD[(String, Array[String])], sampleSize: Int): Unit = {
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

}


