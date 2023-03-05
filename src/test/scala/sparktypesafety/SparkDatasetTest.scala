package sparktypesafety

import org.apache.spark.SparkException
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import sparktypesafety.testcommon.SparkTestBase

import java.io.File
import scala.collection.JavaConverters._

case class OriginalRow(stringValue: String, intValue: Int)

case class SubsetRow(stringValue: String)

case class ConflictingRow(stringValue: String, intValue: Long)

class SparkDatasetTest extends SparkTestBase with BeforeAndAfter with Matchers {

  lazy val spark: SparkSession = sparkSession

  private val testDataBasePath = "target/test-data"
  private val datasetPath = s"${testDataBasePath}/${this.getClass.getSimpleName}/dataset"

  private val originalRows = List(
    OriginalRow("row-1-str", 1),
    OriginalRow("row-2-str", 2),
    OriginalRow("row-3-str", 3)
  )

  before {
    import scala.reflect.io.Directory
    val dir = new Directory(new File(testDataBasePath))
    dir.deleteRecursively()
  }

  behavior of "Spark Dataset"

  it should "successfully read data if schemas match" in {
    // Given
    writeOriginalData()

    // When
    val ds = readDataset(Encoders.product[OriginalRow])

    // Then
    val actual = ds.collectAsList().asScala.toList
    actual should contain theSameElementsAs originalRows
  }

  it should "successfully read data if the schema is a subset of what is written" in {
    // Given
    writeOriginalData()

    // When
    val ds = readDataset(Encoders.product[SubsetRow])

    // Then
    val expected = Seq(
      SubsetRow("row-1-str"),
      SubsetRow("row-2-str"),
      SubsetRow("row-3-str")
    )

    val actual = ds.collectAsList().asScala.toList
    actual should contain theSameElementsAs expected
  }

  it should "throw if schemas do not match" in {
    // Given
    writeOriginalData()

    // When
    val ds = readDataset(Encoders.product[ConflictingRow])

    // Then
    assertThrows[SparkException] {
      ds.collectAsList().asScala.toList
    }
  }

  private def writeOriginalData(): Unit = {
    import spark.implicits._
    val ds = originalRows.toDS
    ds.write.parquet(datasetPath)
  }

  private def readDataset[T](encoder: Encoder[T]): Dataset[T] = {
    implicit val typeEncoder = encoder
    spark.read
      .schema(encoder.schema)
      .parquet(datasetPath)
      .as[T]
  }
}
