import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{min, max, col}

def multiThreadJdbc(key: String,
                obj: String,
                url: String,
                driver: String,
                user: String,
                password: String,
                partitions: Int): org.apache.spark.sql.DataFrame = {
    
    val col = key
    val table = obj
    
    val split = "(SELECT min(%s), max(%s) FROM  %s) AS QUERY".format(col, col, table)
    val bounds = (spark
          .read
          .format("jdbc")
          .option("url", url)
          .option("dbtable", split)
          .option("driver", driver)
          .option("user", user)
          .option("password", password)
          .load()
          .first())
    
    val query = "(SELECT * FROM  %s) AS QUERY".format(table)
    val df = (spark
              .read
              .format("jdbc")
              .option("url", url)
              .option("dbtable", query)
              .option("driver", driver)
              .option("user", user)
              .option("password", password)
              .option("numPartitions", partitions)
              .option("partitionColumn", key)
              .option("lowerBound", bounds.get(0).asInstanceOf[Int])
              .option("upperBound", bounds.get(1).asInstanceOf[Int])
              .load())
    return df
}

/*
val df = multiThreadJdbc(
    "KEY",
    "(SELECT * FROM \"DATABASE\".\"TABLE") as QUERY",
    "jdbc:db://HOST:PORT",
    "driver",
    "user",
    "password",
    int(numPartitions))
*/
