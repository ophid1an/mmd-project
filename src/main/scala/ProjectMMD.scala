import org.apache.spark.sql.SparkSession

case class Basket(b: Array[String])

object ProjectMMD {
  def main(args: Array[String]): Unit = {
    // Create spark session
    val spark = SparkSession
      .builder()
      .appName("ProjectMMD")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Suppress info messages
    sc.setLogLevel("ERROR")

    val groceriesFilename = "groceries.csv"
    val productsFilename = "products-categorized.csv"

    //    val basketsDF = spark.read.format("csv").load(groceriesFilename)
    //    val productsDF = spark.read.format("csv").load(productsFilename)

    val basketsRDD = sc.
      textFile(groceriesFilename).
      map(_.trim.split(',').
        map(_.trim)
      ).
      cache()

    val productsRDD = sc.
      textFile(productsFilename).
      map(_.trim.split(',').
        map(_.trim)
      ).
      map(x =>
        (x(0),
          x(1).split('/').
            map(_.trim)
        )
      ).
      cache()


    showStatistics()

    spark.stop()

    def showStatistics(): Unit = {
      val basketsCnt = basketsRDD.count()
      val basketsSizes = basketsRDD.map(_.size)
      val productsCnt = productsRDD.count()

      //  basketsRDD.reduce((x1, x2) => (x1.toBuffer ++= x2).toArray)
      //  productsRDD.reduce((x1, x2) => (Array(x1(0)).toBuffer ++= Array(x2(0))).toArray)

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
