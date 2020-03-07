
import org.apache.spark.sql.types.{StructType, StringType, LongType, DoubleType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQuery

/*
spark-submit \
--class "StreamingApp" \
--master local[8] \
target/scala-2.11/simple-project_2.11-1.0.jar \
10 \
"/Users/grp/sparkTheDefinitiveGuide/data/activity-data-sample/" \
1 \
"/Users/grp/Desktop/spark-apps/junk/cp" \
"5 seconds" \
"/Users/grp/Desktop/spark-apps/parquet/data.parquet" \
300000
*/

object StreamingApp {
        
        def main(args: Array[String]) {
            
            val maxFilesPerTrigger = args(0).toInt
            val path = args(1)
            val repartition = args(2).toInt
            val checkpointPath = args(3)
            val trigger = args(4)
            val target = args(5)
            val n = args(6).toInt
        
        /*
        val maxFilesPerTrigger = 10
        val path = "/Users/grp/sparkTheDefinitiveGuide/data/activity-data-sample/"
        val repartition = 1
        val checkpointPath = "/Users/grp/Desktop/spark-apps/junk/cp"
        val trigger = "10 seconds"
        val target = "/Users/grp/Desktop/spark-apps/parquet/data.parquet"
        val n = 300000 // 5 min
        */

            val jsonDf = readJson(maxFilesPerTrigger, path)
            val parquetDf = writeParquet(jsonDf, repartition, checkpointPath, trigger, target)
            stop(n)
            kill()

        }

        val spark = (SparkSession
                .builder()
                .appName("sss")
                .master("local[8]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions", 5)
                .getOrCreate())

        val schema = (new StructType()
                .add("Arrival_Time", LongType)
                .add("Creation_Time", LongType)
                .add("Device", StringType)
                .add("Index", LongType)
                .add("Model", StringType)
                .add("User", StringType)
                .add("gt", StringType)
                .add("x", DoubleType)
                .add("y", DoubleType)
                .add("z", DoubleType))

        def readJson(maxFilesPerTrigger: Int, path: String): DataFrame = {
                val jsonDf = spark
                .readStream
                .schema(schema)
                .option("maxFilesPerTrigger", maxFilesPerTrigger)
                .json(path)
                return jsonDf
        }

        def writeParquet(df: DataFrame, repartition: Int, checkpointPath: String, trigger: String, target: String): StreamingQuery = {
                val parquetDf = df
                .repartition(repartition)
                .writeStream
                .option("checkpointLocation", checkpointPath)
                .trigger(Trigger.ProcessingTime(trigger))
                .format("parquet")
                .outputMode("append")
                .option("path", target)
                .start()
                //parquetDf.awaitTermination()
                return parquetDf
        }

        def stop(n: Int): Unit = {
        	    return Thread.sleep(n)
        }

        def kill(): Unit = {
                return spark.streams.active.foreach(_.stop()); spark.stop()
        }

}