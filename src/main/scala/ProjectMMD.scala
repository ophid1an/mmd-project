import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

import scala.util.Random

//case class Products(prods: Map[String, Array[String]]) {
//  def getClass(prodName: String): String = {
//    prods.get(prodName) match {
//      case Some(arr) => arr(0)
//      case None => "No such product!"
//    }
//  }
//
//  def getSubclass(prodName: String): String = {
//    prods.get(prodName) match {
//      case Some(arr) => if (arr.length > 1) arr(1) else arr(0)
//      case None => "No such product!"
//    }
//  }
//}


case class Taxonomy(cl: String = "", subCl: String = "", subSubCl: String = "")

object Taxonomy {
  def apply(cl: String, subCl: String): Taxonomy = apply(cl, subCl, subCl)

  def apply(cl: String): Taxonomy = apply(cl, cl, cl)

  def apply(arr: Array[String]): Taxonomy = arr match {
    case Array(a) => apply(a)
    case Array(a, b) => apply(a, b)
    case Array(a, b, c, _*) => apply(a, b, c)
  }
}

case class Product(name: String = "", taxonomy: Taxonomy = Taxonomy())

case class Customer[A, B](id: A, vec: scala.collection.mutable.Map[B, Int]) {
  private var cnt = 0

  def countItems: Int = cnt

  def increment(subCl: B): Int = {
    val currAmount = vec.get(subCl)
    currAmount match {
      case Some(i) =>
        val newAmount = i + 1
        vec.update(subCl, newAmount)
        cnt += 1
        newAmount
      case _ => -1
    }
  }
}

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

    val groceriesFilename = "groceries.csv"
    val productsFilename = "products-categorized.csv"
    val customersCard = 100
    val seed = 1
    val rand = new Random(seed)

    // Set number of partitions for RDDs equal to cores number
    //    val numPartitions = spark.sparkContext.defaultParallelism
    val numPartitions = 100


    val basketsRDD = sc
      .textFile(groceriesFilename)
      .map(_.trim.split(',')
        .map(_.trim)
      )
      .cache()

    val assignedBasketsRDD = basketsRDD
      .map((rand.nextInt(customersCard), _))
      .partitionBy(new HashPartitioner(numPartitions))
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

    val products = productsRDD.collect().map(x => (x._1, Product(x._1, Taxonomy(x._2)))).toMap
    val productsB = sc.broadcast(products)
    val subClasses = products.values.map(x => x.taxonomy.subCl).toArray
    val subClassesB = sc.broadcast(subClasses)

    // Work in each partition of partitioned RDD separately
    // and cache the result
    //    val customers = assignedBasketsRDD.mapPartitions({ iter =>
    //      iter.toArray.foldLeft(0)((acc, (x, y) => acc + x))
    //      iter
    //    }, preservesPartitioning = true)
    //      .cache()


    displayStats()

    spark.stop()

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

    def displayStats(): Unit = {
      val basketsCnt = basketsRDD.count()
      val basketsSizes = basketsRDD.map(_.length)
      val productsCnt = productsRDD.count()

      println("**********")
      println("Statistics")
      println("**********")

      println("\nBaskets")
      println("------------------------------")
      println("Count: " + basketsCnt)

      println("First 5 baskets: ")
      for (i <- basketsRDD.take(5)) println("\t" + i.mkString(", "))

      println("First 5 baskets sizes: " + basketsSizes.take(5).mkString(", "))
      println("Min size: " + basketsSizes.min())
      println("Max size: " + basketsSizes.max())
      println("Mean size: " + basketsSizes.mean())

      println("\nProducts")
      println("------------------------------")
      println("Count: " + productsCnt)

      println("First 5 products: ")
      for (i <- productsRDD.take(5)) println("\t" + i._1 + " -> " + i._2.mkString(", "))

    }
  }
}
