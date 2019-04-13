import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Mock").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.sql("select * from user_visit_action uv join user_info ui on uv.user_id = ui.user_id").show()
  }
}
