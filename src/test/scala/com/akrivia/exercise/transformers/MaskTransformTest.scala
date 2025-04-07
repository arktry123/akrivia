import com.akrivia.exercise.transformers.MaskTransform
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MaskTransformTest extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("MaskTransformTest")
    .master("local[*]") // Ensure local mode for testing
    .getOrCreate()

  test("test mask transformation for existing column and verify auditDF") {
    import spark.implicits._

    val data = Seq(
      (1, "John Doe", "London", "10001", "Flu"),
      (2, "Jane Smith", "Manchester", "90001", "Cold")
    )

    val df = data.toDF("patientId", "patientName", "city", "postCode", "disease")

    val maskTransform = new MaskTransform("disease", "N/A")
    val (transformedDF, auditDF) = maskTransform.apply(df)

    assert(transformedDF.filter($"disease" === "N/A").count() == 2) // Ensure the disease column is masked

    assert(auditDF.count() == 2) // There should be two rows in the audit log (one for each patient)

    val auditData = auditDF.collect()

    auditData.foreach { row =>
      val oldValue = row.getAs[String]("old_value")
      val newValue = row.getAs[String]("new_value")
      val transformTag = row.getAs[String]("transform_tag")

      assert(transformTag == "mask")
      assert(newValue == "N/A")
      assert(oldValue != newValue)
    }
  }


  test("test mask on non-existing column") {
    import spark.implicits._

    val data = Seq(
      (1, "John Doe", "London", "10001", "Flu"),
      (2, "Jane Smith", "Manchester", "90001", "Cold")
    )

    val df = data.toDF("patientId", "patientName", "city", "postCode", "disease")

    val maskTransform = MaskTransform("abc", "N/A")
    val (transformedDF, auditDF) = maskTransform.apply(df)

    assert(transformedDF.collect().sameElements(df.collect()))

    assert(df.count() == transformedDF.count(), "The number of rows should be the same.")
    assert(auditDF.isEmpty)
  }

}
