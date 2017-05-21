import scala.util.control.Breaks._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row

import scala.util.control.Breaks




object new_customer_extract {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("customer_extract")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    import java.io._
    val writer = new PrintWriter(new File("/root/re.txt"))
    import spark.implicits._
    val df = spark.read.json("hdfs://master:8020/log1/log.log")
    df.createOrReplaceTempView("data")
    // Global temporary view is tied to a system preserved database `global_temp`


    //获取所有用户Mac地址
    val mac_array = spark.sql("SELECT  DISTINCT mac FROM data").collect()


    var i = 0
    val inner = new Breaks
    val lenth = mac_array.length
    //对每一个用户（Mac）进行循环
    while (i < lenth) {
      var result_string = ""
      var mac = mac_array(i)(0)
      var sql = "SELECT  `time` from data where mac = '" + mac + "' order by `time` "
      val time_array = spark.sql(sql).collect()


      //由  time_array 得到 time_list
      import scala.collection.mutable.ListBuffer
      var time_list = new ListBuffer[Int]
      var list_length = time_array.length
      var j = 0
      while(j < list_length){

        time_list += time_array(i)(0).toString.toInt

        j = j + 1
      }

      //循环time_list,得到每一个mac的visit记录
      var k = 0
      var old_time = 0
      var new_time = 0
      //max_visit_time_interval 该字段表示 若前后相邻时间超过该数值即构成 一次访问，即为两次访问的分割点
      var max_visit_time_interval = 300
      var start_time = 0
      var leave_time = 0
      while(k < list_length){
        if(k == 0){
          old_time = time_list(0)
          new_time = time_list(0)
          start_time = time_list(0)

        }else {
          new_time = time_list(k)
          if((new_time - old_time)> max_visit_time_interval){
            leave_time = old_time
            var stay_time = leave_time - start_time
            result_string += """{"mac":"""" + mac +"""",""" +""""in_time":""" + start_time + "," +""""out_time":""" + leave_time + "," +""""stay_time":""" + stay_time + "}\n"
            start_time = new_time
            old_time = new_time


          }else {
            old_time = new_time
          }
        }

        k = k + 1
      }



     //将结果存入文件中
      writer.write(result_string)

      i = i + 1
    }
    //将结果集 存入 文件

    writer.close()


  }
}
