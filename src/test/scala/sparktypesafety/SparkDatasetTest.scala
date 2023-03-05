package sparktypesafety

import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}
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

  it should "successfully process data if schemas match" in {
    // Given
    writeOriginalData()

    // When
    val ds = readDataset(Encoders.product[OriginalRow])
    val actual = ds.collectAsList().asScala.toList

    // Then
    actual should contain theSameElementsAs originalRows
  }

  it should "successfully process data if the schema is a subset of what was written" in {
    // Given
    writeOriginalData()
    // When
    val ds = readDataset(Encoders.product[SubsetRow])
    val actual = ds.collectAsList().asScala.toList

    // Then
    val expected = Seq(
      SubsetRow("row-1-str"),
      SubsetRow("row-2-str"),
      SubsetRow("row-3-str")
    )

    actual should contain theSameElementsAs expected
  }

  it should "successfully process data if mismatched columns aren't output" in {
    // Given
    writeOriginalData()

    // When
    val ds = readDataset(Encoders.product[ConflictingRow])
    val actual = ds.drop(col("intValue")).collectAsList().asScala.toList

    // Then
    val expected = List(
      Row("row-1-str"),
      Row("row-2-str"),
      Row("row-3-str")
    )

    actual should contain theSameElementsAs expected
  }

  it should "throw if mismatched columns are output" in {
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
