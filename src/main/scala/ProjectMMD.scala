import org.apache.spark.sql.SparkSession

object ProjectMMD {
  def main(args: Array[String]): Unit = {

    println("***********************************************************************************************")

    // Create spark session
    val spark = SparkSession
      .builder()
      .appName("ProjectMMD")
      .master("local[*]")
      .getOrCreate()

    // Suppress info messages
    spark.sparkContext.setLogLevel("ERROR")

    println("Hello, Spark!")
    spark.stop()

    println("***********************************************************************************************")
  }
}
