package sparktypesafety.testcommon

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

trait SparkTestBase extends AnyFlatSpecLike with BeforeAndAfterAll {

  implicit var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .config("spark.debug.maxToStringFields", 100)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    sparkSession.sql("set spark.sql.shuffle.partitions=4")
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    super.afterAll()
  }
}
