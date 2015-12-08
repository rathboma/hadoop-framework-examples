package main.java.com.matthewrathbone.sparktest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Optional;

public class ExampleJob {
    private static JavaSparkContext sc;
    
    public ExampleJob(JavaSparkContext sc){
    	this.sc = sc;
    }
    
    public static final PairFunction<Tuple2<Integer, Optional<String>>, Integer, String> KEY_VALUE_PAIRER =
    new PairFunction<Tuple2<Integer, Optional<String>>, Integer, String>() {
    	public Tuple2<Integer, String> call(
    			Tuple2<Integer, Optional<String>> a) throws Exception {
			// a._2.isPresent()
    		return new Tuple2<Integer, String>(a._1, a._2.get());
    	}
	};
	
	public static JavaRDD<Tuple2<Integer,Optional<String>>> joinData(JavaPairRDD<Integer, Integer> t, JavaPairRDD<Integer, String> u){
        JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = t.leftOuterJoin(u).values().distinct();
        return leftJoinOutput;
	}
	
	public static JavaPairRDD<Integer, String> modifyData(JavaRDD<Tuple2<Integer,Optional<String>>> d){
		return d.mapToPair(KEY_VALUE_PAIRER);
	}
	
	public static Map<Integer, Object> countData(JavaPairRDD<Integer, String> d){
        Map<Integer, Object> result = d.countByKey();
        return result;
	}
	
	public static JavaPairRDD<String, String> run(String t, String u){
        JavaRDD<String> transactionInputFile = sc.textFile(t);
        JavaPairRDD<Integer, Integer> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) {
                String[] transactionSplit = s.split("\t");
                return new Tuple2<Integer, Integer>(Integer.valueOf(transactionSplit[2]), Integer.valueOf(transactionSplit[1]));
            }
        });
        
        JavaRDD<String> customerInputFile = sc.textFile(u);
        JavaPairRDD<Integer, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String[] customerSplit = s.split("\t");
                return new Tuple2<Integer, String>(Integer.valueOf(customerSplit[0]), customerSplit[3]);
            }
        });

        Map<Integer, Object> result = countData(modifyData(joinData(transactionPairs, customerPairs)));
        
        List<Tuple2<String, String>> output = new ArrayList<>();
	    for (Entry<Integer, Object> entry : result.entrySet()){
	    	output.add(new Tuple2<>(entry.getKey().toString(), String.valueOf((long)entry.getValue())));
	    }

	    JavaPairRDD<String, String> output_rdd = sc.parallelizePairs(output);
	    return output_rdd;
	}
	
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));
        ExampleJob job = new ExampleJob(sc);
        JavaPairRDD<String, String> output_rdd = job.run(args[0], args[1]);
        output_rdd.saveAsHadoopFile(args[2], String.class, String.class, TextOutputFormat.class);
        sc.close();
    }
}
