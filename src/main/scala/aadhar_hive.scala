import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types

import org.apache.spark.sql.SaveMode
object aadhar_hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder.
      master("yarn-client").
      appName("AADHAR DATASET ANALYSIS").enableHiveSupport().getOrCreate()
    import spark.implicits._
     val aadhar1 = spark.read.csv("/user/jithushyam/aadhaar_data.csv")
      .toDF("Date_1", "Registrar", "PrivateAgency","State","District","SubDistrict","Pincode",
        "Gender","Age","AadharGenerated","Rejected","MbleNo","Email")
    val aadhar=aadhar1.withColumn("uniqe_id",monotonically_increasing_id().cast("String")).
      withColumn("State_Name",$"State").drop("State")
    val df=aadhar.selectExpr("Date_1", "Registrar", "PrivateAgency","District","SubDistrict","Pincode","Gender",
      "cast(Age as int) as Age", "cast(AadharGenerated as int) as AadharGenerated","cast(Rejected as int) as Rejected","MbleNo","Email","uniqe_id",
      "State_Name"
    )
     spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict" )
    df.write.mode(SaveMode.Append).insertInto("jithu_hive.aadhar")
  }
}

