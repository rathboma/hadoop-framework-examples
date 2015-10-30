package main.java.com.matthewrathbone.sparktest;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.network.shuffle.ShuffleClient;


import scala.Tuple2;
import scala.Predef;
import com.google.common.base.Optional;

public class SparkJoins {
    @SuppressWarnings("serial")
    
    public static final PairFunction<Tuple2<Integer, Optional<String>>, Integer, String> KEY_VALUE_PAIRER =
    new PairFunction<Tuple2<Integer, Optional<String>>, Integer, String>() {
    	public Tuple2<Integer, String> call(
    			Tuple2<Integer, Optional<String>> a) throws Exception {
			// a._2.isPresent()
    		return new Tuple2<Integer, String>(a._1, a._2.get());
    	}
	};
    
    public static void main(String[] args) throws FileNotFoundException {
    	// SPARK_USER
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));

        JavaRDD<String> transactionInputFile = sc.textFile(args[0]);
        JavaPairRDD<Integer, Integer> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) {
                String[] transactionSplit = s.split("\t");
                return new Tuple2<Integer, Integer>(Integer.valueOf(transactionSplit[2]), Integer.valueOf(transactionSplit[1]));
            }
        });
        
        JavaRDD<String> customerInputFile = sc.textFile(args[1]);
        JavaPairRDD<Integer, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String[] customerSplit = s.split("\t");
                return new Tuple2<Integer, String>(Integer.valueOf(customerSplit[0]), customerSplit[3]);
            }
        });

        //Left Outer join operation
        JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = transactionPairs.leftOuterJoin(customerPairs).values().distinct();
        System.out.println("LeftOuterJoins function Output: "+leftJoinOutput.collect());
        JavaPairRDD<Integer, String> res = leftJoinOutput.mapToPair(KEY_VALUE_PAIRER);
        System.out.println("MapToPair function Output: "+res.collect());
        Map<Integer, Object> result = res.countByKey();
        System.out.println("CountByKey function Output: "+result.toString());
        
        List<Tuple2<Integer, Long>> output = new ArrayList<>();
	    for (Entry<Integer, Object> entry : result.entrySet()){
	    	output.add(new Tuple2<>(entry.getKey(), (long)entry.getValue()));
	    }

	    JavaPairRDD<Integer, Long> output_rdd = sc.parallelizePairs(output);
	    output_rdd.saveAsHadoopFile(args[2], Integer.class, String.class, TextOutputFormat.class);

        sc.close();
    }
}
