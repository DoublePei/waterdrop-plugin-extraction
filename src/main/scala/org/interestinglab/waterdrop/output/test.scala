package org.interestinglab.waterdrop.output

import org.apache.spark.{SparkConf, SparkContext}
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
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val value = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    value.foreach(println(_))

    //    val a= ds.collect()(0).getAs[Long](0);
    //    if (a == null || a.equals("null")) {
    //      lower=0l
    //      upper=0l
    //    }else {
    //      lower = ds.collect()(0).getAs[Long](0)
    //      upper = ds.collect()(0).getAs[Long](1)
    //    }
    sc.stop()
  }
}
