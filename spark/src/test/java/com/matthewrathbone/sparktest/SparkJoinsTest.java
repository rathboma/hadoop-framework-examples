package test.java.com.matthewrathbone.sparktest;

import java.io.File;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import main.java.com.matthewrathbone.sparktest.SparkJoins;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.io.Files;

public class SparkJoinsTest implements Serializable {
	  private transient JavaSparkContext sc;
	  private transient File tempDir;

	  @Before
	  public void setUp() {
	    sc = new JavaSparkContext("local", "SparkJoinsTest");
	    tempDir = Files.createTempDir();
	    tempDir.deleteOnExit();
	  }

	  @After
	  public void tearDown() {
	    sc.stop();
	    sc = null;
	  }
	  
	  @Test
	  public void sortByKey() {
	    List<Tuple2<Integer, Integer>> transactions = new ArrayList<>();
	    transactions.add(new Tuple2<>(1, 1));
	    transactions.add(new Tuple2<>(2, 1));
	    transactions.add(new Tuple2<>(2, 1));
	    transactions.add(new Tuple2<>(3, 2));
	    transactions.add(new Tuple2<>(3, 1));
	    
	    List<Tuple2<Integer, String>> users = new ArrayList<>();
	    users.add(new Tuple2<>(1, "US"));
	    users.add(new Tuple2<>(2, "GB"));
	    users.add(new Tuple2<>(3, "FR"));

	    JavaPairRDD<Integer, Integer> transactions_rdd = sc.parallelizePairs(transactions);
	    JavaPairRDD<Integer, String> users_rdd = sc.parallelizePairs(users);

	    JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = SparkJoins.joinData(transactions_rdd, users_rdd);
	    
	    Assert.assertEquals(4, leftJoinOutput.count());
	    JavaPairRDD<Integer, String> res = SparkJoins.modifyData(leftJoinOutput);
	    List<Tuple2<Integer, String>> sortedRes = res.sortByKey().collect();
	    Assert.assertEquals(1, sortedRes.get(0)._1.intValue());
	    Assert.assertEquals(1, sortedRes.get(1)._1.intValue());
	    Assert.assertEquals(1, sortedRes.get(2)._1.intValue());
	    Assert.assertEquals(2, sortedRes.get(3)._1.intValue());
	    
	    Map<Integer, Object> result = SparkJoins.countData(res);
	    Assert.assertEquals((long)3, result.get(1));
	    Assert.assertEquals((long)1, result.get(2));
	    
	  }
}
