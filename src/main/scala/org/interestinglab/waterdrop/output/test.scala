package org.interestinglab.waterdrop.output

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]): Unit = {

    //    val spark = SparkSession
    //      .builder()
    //      .appName("wangxinjun_midu_age_pred")
    //      .master("local[4]")
    //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .config("spark.rdd.compress", true)
    //      .config("spark.sql.crossJoin.enabled", true)
    //      //      .config(sc.getConf)
    //      .getOrCreate()
    //
    //    import spark.implicits._
    //
    //    val df: Dataset[Row] = spark.read.json("people.json")
    //
    // val arr=  df.schema.fields(0).name;
    //      print(arr)
    //
    //    val s = "aa vv cc, dd ,dd"
    //    val c=s.replaceAll("\\s","");
    //    println(c)

//    var array = ArrayBuffer[(String, String)]()
//
//    val agg: Long = (90 - 2) / 8
//    for (i <- 0 to 8) {
//      val end = i + 1;
//      array += (((i * agg).toString, (end * agg).toString))
//    }
//    val predicates: ArrayBuffer[String] = array.map { case (start, end) => s" id>= $start and id < $end"
//    }
//    val arr = predicates.toArray
//
//    arr.foreach(println(_))

    val c =Math.ceil((375d-1d)/100d)
    println(c)
  }
}
