package main.scala.com.matthewrathbone.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.rdd.RDD
import scala.collection.Map

object Main {
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
	val sc = new SparkContext(conf)
	
	val transactions = sc.textFile(args(0))
	val newTransactionsPair = transactions.map{t =>                
	    val p = t.split("\t")
	    (p(2).toInt, p(1).toInt)
	}
	
	val users = sc.textFile(args(1))
	val newUsersPair = users.map{t =>                
	    val p = t.split("\t")
	    (p(0).toInt, p(3))
	}
	
	val result = processData(newTransactionsPair, newUsersPair)
	val r = sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))
	
	r.saveAsTextFile(args(2))
  }
  
  
  def processData (t: RDD[(Int, Int)], u: RDD[(Int, String)]) : Map[Int,Long] = {
	var jn = t.leftOuterJoin(u).values.distinct
	return jn.countByKey
  }
}