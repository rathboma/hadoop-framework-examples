package test.java.com.matthewrathbone.sparktest;

import java.io.Serializable;
import main.java.com.matthewrathbone.sparktest.ExampleJob;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparkJavaJoinsTest implements Serializable {
	private static final long serialVersionUID = 1L;
	private transient JavaSparkContext sc;

	@Before
	public void setUp() {
		sc = new JavaSparkContext("local", "SparkJoinsTest");
	}

	@After
	public void tearDown() {
		if (sc != null){
			sc.stop();
		}
	}

	@Test
	public void testExampleJob() {
		ExampleJob job = new ExampleJob(sc);
		JavaPairRDD<String, String> result = job.run("./transactions.txt", "./users.txt");
		    
		Assert.assertEquals("1", result.collect().get(0)._1);
		Assert.assertEquals("3", result.collect().get(0)._2);
		Assert.assertEquals("2", result.collect().get(1)._1);
		Assert.assertEquals("1", result.collect().get(1)._2);
	}
}
