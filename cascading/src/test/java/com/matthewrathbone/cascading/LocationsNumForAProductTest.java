package com.matthewrathbone.cascading;

import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.matthewrathbone.cascading.LocationsNumForAProduct;



import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class LocationsNumForAProductTest extends CascadingTestCase{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    public LocationsNumForAProductTest () {
        super( "distinct locations count tests" );
    }

    public void testMatthewFiles() throws IOException {
    	String usersString = 
    	          "1\tmatthew@test.com\tEN\tUS\n"
    	        + "2\tmatthew@test2.com\tEN\tGB\n"
    	        + "3\tmatthew@test3.com\tFR\tFR";

        String transactionString = 
    	          "1\t1\t1\t300\ta jumper\n"
    	        + "2\t1\t2\t300\ta jumper\n"
    	        + "3\t1\t2\t300\ta jumper\n"
    	        + "4\t2\t3\t100\ta rubber chicken\n"
    	        + "5\t1\t3\t300\ta jumper";

        Fields users_fields = new Fields( "id", "email", "language", "location" );
        Tap users = new Lfs( new TextDelimited( users_fields, false, "\t" ), createTempFile(usersString, "users_test.txt"));

        Fields transactions_fields = new Fields( "transaction-id", "product-id", "user-id", "purchase-amount", "item-description" );
        Tap transactions = new Lfs( new TextDelimited(transactions_fields, false, "\t" ), createTempFile(transactionString, "transactions_test.txt"));
        Tap sink = new Lfs( new TextDelimited( false, "\t" ), "target/output_folder_casc", true );

        Properties properties = new Properties();
	    AppProps.setApplicationJarClass( properties, LocationsNumForAProduct.class );
	    FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );
        
        FlowDef flowDef = LocationsNumForAProduct.createWorkflow(users, transactions, sink);
        Flow flow = flowConnector.connect( flowDef );
        flow.complete();

        validateLength(flow, 2);

        List<Tuple> results = flowResultsMap(flow);
        Tuple t1 = results.get(0);
        Tuple t2 = results.get(1);

        Map<Integer, Integer> res = new HashMap<Integer, Integer>();
        res.put(t1.getInteger(0), t1.getInteger(1));
        res.put(t2.getInteger(0), t2.getInteger(1));

        assertEquals("product 1", 3, res.get(1).intValue());
        assertEquals("product 2", 1, res.get(2).intValue());
    }
    
    String createTempFile(String contents, String name) throws FileNotFoundException, UnsupportedEncodingException {
    	File f = new File(name);
    	
    	PrintWriter writer = new PrintWriter(new File(name), "UTF-8");
    	writer.println(contents);
    	writer.close();
    	
    	return f.getAbsolutePath();
    	
    }
    
    List flowResultsMap(Flow flow) throws IOException {
    	TupleEntryIterator iterator = flow.openSink();
    	
    	List<Tuple> result = new ArrayList<Tuple>();
        
        while (iterator.hasNext()){
        	TupleEntry entry = iterator.next();
        	Tuple t = new Tuple();
        	t.addAll(entry.getTuple());
        	result.add(t);
        }
        
        return result;
    }
}