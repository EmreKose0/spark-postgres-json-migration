import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.sql.DriverManager
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import java.util.Properties

object DataTransfer{

    def main(args: Array[String]): Unit = {
        
        println("Spark App Starting...")

        val spark = SparkSession.builder
                    .appName("PostgreSQL Data Transfer")
                    .master("local[*]")
                    .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")


        val jdbcUrl = "jdbc:postgresql://IP:Port/postgres"
        val connectionProperties = new Properties()
        connectionProperties.put("user", "user")
        connectionProperties.put("password","password")
        connectionProperties.put("driver","org.postgresql.Driver")

        connectionProperties.put("fetchsize", "10000") // Batch size
        connectionProperties.put("batchsize", "10000")
        connectionProperties.put("connectTimeout", "30")
        connectionProperties.put("socketTimeout", "60")

        println("Data is Reading (Partitioned)...")

      val sourceDataFrame = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "table")
        .option("user", "user")
        .option("password", "password")
        .option("driver", "org.postgresql.Driver")
        .option("numPartitions", "10") // 10 paralel okuma
        .option("partitionColumn", "id") // PRIMARY KEY kolonunu buraya yaz
        .option("lowerBound", "1") // Min ID değeri
        .option("upperBound", "10000000") // Max ID değeri (tahmini)
        .option("fetchsize", "10000")
        .load()

      println("--- First 5 Rows Check ---")
      sourceDataFrame.show(5)

      println("--- Schema Check ---")
      sourceDataFrame.printSchema()

      println("--- Calculating All Rows Count ---")
      val totalCount = sourceDataFrame.count()
      println(s"Total $totalCount rows readed")

      println(s"Data Writing: number_spolia.web_socket_logs_test table...")
      val startTime = System.currentTimeMillis()

      val truncateQuery = "TRUNCATE TABLE target table"
      val connection = DriverManager.getConnection(jdbcUrl, connectionProperties)
      try {
        val statement = connection.createStatement()
        statement.execute(truncateQuery)
        println("Table Truncated (TRUNCATE)")
      } finally {
        connection.close()
      }

      // JSON kolonlarını cast et
      val dfWithJsonCast = sourceDataFrame
        .withColumn("messageinfo", col("messageinfo").cast(StringType))
        .withColumn("wholemessage", col("wholemessage").cast(StringType))

      val jdbcUrlWithOptions = jdbcUrl

      dfWithJsonCast.write
        .mode(SaveMode.Append)
        .option("batchsize", "10000")
        .option("isolationLevel", "NONE")
        .option("stringtype", "unspecified") // PostgreSQL'in otomatik cast yapmasını sağla
        .jdbc(jdbcUrl, "table", connectionProperties)

      val endTime = System.currentTimeMillis()
      val time = (startTime - endTime) / 1000.0 // saniye cinsinden

      println(f"⏱ Time: $time%.2f second (${time/60}%.2f minutes)")


      println("✓ Process Done!")

      println("--- Total Count is Checking on Target Table---")
      val targetCount = spark.read
        .jdbc(jdbcUrl, "target table", connectionProperties)
        .count()
      println(s"Target Table has $targetCount rows")

        spark.stop

    }
}