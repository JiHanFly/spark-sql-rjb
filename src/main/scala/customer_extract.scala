
import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row

import scala.util.control.Breaks




object customer_extract {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("customer_extract")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("/spark_data/spark_sql_data.json")
    df.createOrReplaceTempView("data")
    // Global temporary view is tied to a system preserved database `global_temp`


    //获取所有用户Mac地址
    val distinct_mac_DF = spark.sql("SELECT  DISTINCT mac FROM data")
    val mac_array = distinct_mac_DF.collect()


    var i = 0
    var result_string = ""
    val outer = new Breaks
    val inner = new Breaks
    val every_visit = new Breaks
    //对每一个用户（Mac）进行循环
    while (i < mac_array.length) {
      outer.breakable {
        var mac = mac_array(i)
        var sql = "SELECT  `time` from data where mac = '" + mac + "' order by `time`  limit 1"
        var min_time = spark.sql(sql).collect() (0).asInstanceOf[Row].getInt(0)
        sql = "SELECT  `time` from data where mac = '" + mac + "' order by `time` desc limit 1"
        var max_time = spark.sql(sql).collect() (0).asInstanceOf[Row].getInt(0)
        //第一层过滤，过滤掉 只检测到一次的用户
        if (min_time == max_time) {
          outer.break
        }


        var old_num = 0
        var new_num = 0
        var start_time = 0
        var leave_time = 0
        import scala.collection.mutable.ArrayBuffer
        var result_array = new ArrayBuffer[Array[Int]]
        var m = 0 //结果集内条数
        var next_start_time = 0
        var now_time = min_time
        var flag_a = 1
        //当此flag值为 1 时 ，标志 每一个mac 首次访问
        var flag_b = 1 //当此flag值为 1 时，标志 每一次访问开始


        /* 在最小时间和最大时间按照时间间隔从小到大循环*/
        inner.breakable {
          while (now_time < (max_time + 300)) {

            every_visit.breakable {
              sql = "SELECT  count(*) num from data where mac ='" + mac + "' and `time`>" + now_time + ""
              new_num = spark.sql(sql).collect() (0).toString.toInt

              if ((flag_a == 1) && (flag_b == 1)) {
                old_num = new_num
                start_time = min_time
                flag_a = 0
                flag_b = 0

              } else if ((flag_a == 0) && (flag_b == 0)) {

                if (old_num > new_num) {
                  old_num = new_num
                  flag_a = 0
                  flag_b = 0

                } else if (new_num == old_num) {


                  leave_time = now_time - 60
                  //添加到结果集
                  result_array += Array(start_time, leave_time)

                  var sql13 = "SELECT  `time`  next_time from data where mac ='" + mac + "' and `time`> " + leave_time + " order by `time`  limit 1"; //SQL语句


                  if (spark.sql(sql13).collect()(0).asInstanceOf[Row].isNullAt(0)) {

                    inner.break
                  }

                  next_start_time = spark.sql(sql13).collect()(0).asInstanceOf[Row].getInt(0)


                  now_time = next_start_time

                  m = m + 1
                  start_time = next_start_time
                  leave_time = 0
                  flag_a = 0
                  flag_b = 1
                  next_start_time = 0
                  every_visit.break


                }
              } else if ((flag_a == 0) && (flag_b == 1)) {
                //print 4 ."<br>";
                old_num = new_num
                flag_a = 0
                flag_b = 0


              }

              now_time = now_time + 120
            }
          }

        }
        //将 result_array 结果 转换为Json格式 ，存入 result_string
        for (i <- result_array) {
          var vist_string = """{"mac":"""" + mac +"""",""" +""""in_time":""" + i(0) + "," +""""out_time":""" + i(1) + "," +""""stay_time":""" + (i(1) - i(0)) + "}\n"
          result_string =  vist_string
          //将结果集 存入 文件
    import java.io._
    val writer = new PrintWriter(new File("\\sparkdata\\visit_records.json"))

    writer.write(result_string)
    writer.close()

        }

      }
      i = i + 1
    }
    


  }
}
