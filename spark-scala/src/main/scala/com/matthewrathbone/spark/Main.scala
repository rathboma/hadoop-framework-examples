package main.scala.com.matthewrathbone.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat

object Main {
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
	val sc = new SparkContext(conf)
	
	val transactions = sc.textFile("transactions.txt")
	val newTransactionsPair = transactions.map{t =>                
	    val p = t.split("\t")
	    (p(2).toInt, p(1).toInt)
	}
	
	val users = sc.textFile("users.txt")
	val newUsersPair = users.map{t =>                
	    val p = t.split("\t")
	    (p(0).toInt, p(3))
	}
	
	val jn = newTransactionsPair.leftOuterJoin(newUsersPair).values.distinct
	val result = jn.countByKey
	val r = sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))
	
	// org.apache.hadoop.mapred.JobConf
	//r.saveAsHadoopFile("hdfs://172.31.24.86:9000/user/hadoop/spark_result", classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]], sc.hadoopConfiguration, classOf[DefaultCodec]) 
	r.saveAsTextFile("spark_result_scala")
  }
}