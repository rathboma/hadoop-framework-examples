package test.scala.com.matthewrathbone.spark

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Before
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.Test
import scala.collection.Map
import org.apache.spark.SparkContext._

class SparkJoinsTest extends AssertionsForJUnit {
  
  var sc: SparkContext = _
  
  @Before def initialize() {
	 val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
	 sc = new SparkContext(conf)
  }
  
  @Test def sortByKey() {
    val transactions: List[Tuple2[Int, Int]] = List(new Tuple2[Int, Int](1, 1), new Tuple2[Int, Int](2, 1),
        new Tuple2[Int, Int](2, 1), new Tuple2[Int, Int](3, 2), new Tuple2[Int, Int](3, 1))
        
    val users: List[Tuple2[Int, String]] = List(new Tuple2[Int, String](1, "US"), Tuple2[Int, String](2, "GB"), Tuple2[Int, String](3, "FR"))
    
    val transactionsRDD = sc.parallelize(transactions.toSeq)
    val usersRDD = sc.parallelize(users.toSeq)
    
    val jn = transactionsRDD.leftOuterJoin(usersRDD).values.distinct
    
    val result = jn.countByKey

    assert(result.get(1).get === 3)
	assert(result.get(2).get === 1)
  }
}