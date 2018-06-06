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
                     clustersNum: Int = 7,

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
    *
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
    *
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

    // Initialize a random generator
    val rand = new scala.util.Random(params.seed)

    // Method to assign random IDs to customers
    def getRandomId: Int = rand.nextInt(params.customersMaxCard)

    // Load the baskets in RDD
    val basketsRDD = sc
      .textFile(params.basketsPath)
      .map(_.trim.split(',')
        .map(_.trim)
      )

    // Load the products in RDD
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

    // Create Taxonomy object with product related information
    val taxonomy = productsRDD.collect()
      .foldLeft(Taxonomy())((acc, i) => acc ++ Taxonomy(i._1, i._2))

    // Broadcast variables
    val productsB = sc.broadcast(taxonomy.products)
    val productsToSubClassesB = sc.broadcast(taxonomy.productsToSubClasses)
    val subClassesToClassesB = sc.broadcast(taxonomy.subClassesToClasses)
    val classesToSubClassesB = sc.broadcast(taxonomy.classesToSubClasses)

    // Convert each product name in each transaction to its corresponding id
    val convertedBasketsRDD: RDD[Array[Int]] = basketsRDD
      .map(b => b
        .map(prodName => productsB.value.getOrElse(prodName, -1)))
      .cache()

    // Assign each transaction to a customer id
    val assignedBasketsRDD: RDD[(Int, Array[Int])] = convertedBasketsRDD
      .map(b => getRandomId -> b)
      .cache()

    // Transform each product id in each assigned transaction to its corresponding subclass id
    val assignedSubClassesBasketsRDD: RDD[(Int, Array[Int])] = assignedBasketsRDD
      .mapValues(b => b
        .map(prodId => productsToSubClassesB.value.getOrElse(prodId, -1)))
      .cache()

    // Transform each assigned transaction to a customer spending vector and
    // NOTE: Customers cardinality may be less than customersMaxCard
    val assignedAndTransformedBasketsRDD: RDD[(Int, Customer[Int])] = assignedSubClassesBasketsRDD
      .mapValues(b => {
        val clArr = b.map(subClassesToClassesB.value.getOrElse(_, -1))
        Customer(Spending[Int]() ++ Spending(clArr: _*),
          Spending[Int]() ++ Spending(b: _*))
      })

    // Reduce each assigned customer spending vector
    // to get customers absolute spending vectors
    val customersRDD = assignedAndTransformedBasketsRDD.reduceByKey(_ ++ _)

    // and gather them
    val customers = customersRDD.collect().toMap

    val customersCard = customers.size

    // Alert if customers cardinality is less than customersMaxCard
    if (customersCard != params.customersMaxCard)
      println("***** Number of customers doesn't equal customersMaxCard *****")

    // Assertions:

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

    // Get customers fractional spending vectors
    val fractionalCustomers = customers.mapValues(_.fractional)

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

    // // Get normalized customers fractional spending vectors
    val normalizedFractionalCustomers = fractionalCustomers.mapValues(
      c => Customer(c.clSpending * adjustedFractionalClSpendingsTotal,
        c.subClSpending * adjustedFractionalSubClSpendingsTotal)
    )

    /** *******************************
      * ** Association Rules Mining ***
      * *******************************/

    // RDD with distinct subclasses ids for each transaction
    val subClassesRDD: RDD[Array[Int]] = assignedSubClassesBasketsRDD
      .map { case (_, v) => v.distinct }
      .cache()

    // RDD with distinct classes ids for each transaction
    val classesRDD: RDD[Array[Int]] = subClassesRDD
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

        // Compute score for P^(n) vector
        (prodId,
          (Product(Map[Int, Double]())
            + prodIndirectlyAssociatedSubClasses.map(x => x -> .25).toMap // within subclass of associated class
            + prodDirectlyAssociatedSubClasses.map(x => x -> 1.0).toMap // within associated class
            + prodSiblingSubClasses.map(x => x -> 0.5).toMap) // subclass with same class
            + Map(prodSubClass -> 1.0) // within same subclass
        )
      }
    }

    /** *****************
      * ** Clustering ***
      * *****************/

    val parsedData = sc.parallelize(normalizedFractionalCustomers.toSeq.map {
      case (_, v) => v.clSpending.sparseVec(classesToSubClassesB.value.size)
    })

    //    println("\n\n****** Clustering using customers' normalized fractional class spendings ******\n\n")
    //
    //    // Calculate WSSSE for different values of k
    //    Range(1, 21).foreach(clusterSize => {
    //      val clusters = findClusters(parsedData, clusterSize, iterationsNum)
    //      // Evaluate clustering by computing Within Set Sum of Squared Errors
    //      println(s"Cluster size: $clusterSize  WSSSE: ${clusters.computeCost(parsedData)}")
    //    })


    /** *******************************
      * ** Products recommendations ***
      * *******************************/


    val targetVec = normalizedFractionalCustomers.getOrElse(params.target,
      Customer(Spending[Int](), Spending[Int]())).subClSpending.vec

    if (targetVec.isEmpty) {
      println("***** Unfortunately there is no customer with such ID *****")
      sys.exit(1)
    }

    val previousPurchasedProducts: Set[Int] = assignedBasketsRDD.
      filter { case (customerId, _) => 0 == customerId }
      .flatMap { case (_, basket) => basket }
      .collect().toSet
    val targetVecB = sc.broadcast(targetVec)

    val initialResults = transformedProductsRDD.map {
      case (prodId, prod) => prodId -> computeSimilarity(
        targetVecB.value, prod.vec)
    }

    // Sort results by descending similarity
    val sortedResults = initialResults.collect().sortBy(_._2).toList

    // Get filtered results
    val filteredResults = Filter(sortedResults, previousPurchasedProducts, taxonomy)
      .applyFilter.results

    println("\n\n***** Products Recommendations *****\n")
    println(s"For customer id: ${params.target}\n")
    filteredResults.foreach { case (k, v) =>
      val subClassId = taxonomy.productsToSubClasses(k)
      val classId = taxonomy.subClassesToClasses(subClassId)
      val prodName = taxonomy.idsToProducts(k)
      val className = taxonomy.idsToClasses(classId)
      val subClassName = taxonomy.idsToSubClasses(subClassId)
      println(s"ID: $k / Name: $prodName\n\tClass: $className\n\tSubclass: $subClassName\n\tSimilarity: $v")
    }

    spark.stop()

  }

  case class Filter(input: List[(Int, Double)], previousPurchasedProducts: Set[Int],
                    taxonomy: Taxonomy, classes: Map[Int, Int] = Map(),
                    subClasses: Map[Int, Int] = Map(), results: List[(Int, Double)] = List()) {
    val maxProductsPerProductSubClass = 3
    val maxProductsPerProductClass = 5

    // Apply constraints for results
    // 6th line from the bottom of page 11 of paper
    def applyFilter: Filter = {
      if (input.isEmpty)
        this
      else {
        val prodId = input.head._1
        val prodSubClass = taxonomy.productsToSubClasses.getOrElse(prodId, -1)
        val prodClass = taxonomy.subClassesToClasses.getOrElse(prodSubClass, -1)
        val prodSubClassesCard = classes.getOrElse(prodClass, 0)
        val prodClassesCard = classes.getOrElse(prodClass, 0)

        if (previousPurchasedProducts.contains(prodId) ||
          prodSubClassesCard >= maxProductsPerProductSubClass ||
          prodClassesCard >= maxProductsPerProductClass)
          this.copy(input.tail, previousPurchasedProducts, taxonomy,
            classes, subClasses, results)
        else
          this.copy(input.tail, previousPurchasedProducts, taxonomy,
            classes.updated(prodClass, prodClassesCard + 1),
            subClasses.updated(prodSubClass, prodSubClassesCard + 1),
            results ++ List(input.head))
      }
    }
  }

  // Method to compute cosine similarity between two vectors represented as Map objects
  def computeSimilarity(map1: Map[Int, Double], map2: Map[Int, Double]): Double = {
    def dotProduct[A](map1: Map[A, Double], map2: Map[A, Double]): Map[A, Double] =
      map1.foldLeft(map1)((acc, i) => acc.updated(i._1, i._2 * map2.getOrElse(i._1, 0.0)))

    def normL2(m: Map[Int, Double]): Double = Math.sqrt(m.values.map(x => x * x).sum)

    val numerator = dotProduct(map1, map2).values.sum
    val denominator = normL2(map1) * normL2(map2)
    numerator / denominator
  }

  // Method to perform clustering using KMeans
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


