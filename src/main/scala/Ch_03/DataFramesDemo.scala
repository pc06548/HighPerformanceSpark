package Ch_03

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object DataFramesDemo {

  case class RawPanda(id: Long, zip: String, pt: String,
                      happy: Boolean, attributes: Array[Double])
  case class PandaPlace(name: String, pandas: Array[RawPanda])

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("DemoSession")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    import session.implicits._

    val sc: SparkContext = SparkContext.getOrCreate()

    val hiveContext: HiveContext = new HiveContext(sc)

    val sqlContext: SQLContext = new SQLContext(sc)

    val df1: DataFrame = session.read.json("resources/rawpanda.json")

    //df1.printSchema()

    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
    val damao1 = RawPanda(2, "M1B 5K8", "ok", true, Array(0.1, 0.1))
    val damao2 = RawPanda(3, "M1B 5K9", "huge", true, Array(0.1, 0.1))
    val damao3 = RawPanda(4, "M1B 5K8", "ok", true, Array(0.1, 0.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val pandaPlace1 = PandaPlace("india", Array(damao1))
    val pandaPlace2 = PandaPlace("mumbai", Array(damao2))
    val pandaPlace3 = PandaPlace("pune", Array(damao3))
    val df = session.createDataFrame(Seq(pandaPlace, pandaPlace1, pandaPlace2, pandaPlace3))
    //df.printSchema()

    df.filter(df("pandas")(0)("happy") === true)//.show()

    df.filter(df("pandas")(0)("attributes")(0) < 0.5)//.show()

    df.select(df("pandas"),
     when(df("name") === "torontto", 10).
     when(df("name") === "toronto", 11)
       .otherwise(1).as("Won")
    )//.show()

   // df.dropDuplicates("name").show()

    val pandaInfo = df.explode(df("pandas")){
      case Row(pandas: Seq[Row]) =>
        pandas.map{
          case (Row(
          id: Long,
          zip: String,
          pt: String,
          happy: Boolean,
          attrs: Seq[Double])) =>
            RawPanda(id, zip, pt, happy, attrs.toArray)
        }}

    val groupedByZip: RelationalGroupedDataset = pandaInfo.groupBy("zip")

    groupedByZip.agg(sum("id"), min("id"), max("id"))//.show()

    val windowSpec: WindowSpec = Window.orderBy(pandaInfo("id")).partitionBy(pandaInfo("zip")).rangeBetween(2,4)

    pandaInfo.sort(pandaInfo("id").desc).show()


    def toRDD(input: DataFrame): RDD[RawPanda] = {
      val rdd: RDD[Row] = input.rdd
      rdd.map(row => RawPanda(row.getAs[Long](0), row.getAs[String](1),
        row.getAs[String](2), row.getAs[Boolean](3), row.getAs[Array[Double]](4)))
    }


  }
}
