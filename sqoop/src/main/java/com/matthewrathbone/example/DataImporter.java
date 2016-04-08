package com.matthewrathbone.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.tool.ImportTool;
import com.cloudera.sqoop.SqoopOptions;

public class DataImporter {
	
	public static void main(String[] args){
		System.out.println(export());
	}
	
	public static int export(){
		SqoopOptions options = new SqoopOptions();
	    options.setConnectString("jdbc:mysql://localhost/test");
	    
	    //options.setTableName("TABLE_NAME");
	    //options.setWhereClause("id>10");     // this where clause works when importing whole table, ie when setTableName() is used
	   
	    options.setUsername("root");
	    options.setPassword("");
	    options.setNumMappers(2); 
	    options.setSqlQuery("select id, location from m_users WHERE $CONDITIONS");
	    options.setSplitByCol("id");
	    
	    Configuration config = new Configuration(); 
	    config.addResource(new Path("/Users/elena/apache/hadoop-0.20.2/conf/core-site.xml"));
	    config.addResource(new Path("/Users/elena/apache/hadoop-0.20.2/conf/hdfs-site.xml"));
	    
	    options.setConf(config);
	    options.setTargetDir("users_java_10");
	    options.setHadoopHome("/Users/elena/apache/hadoop-0.20.2");
	    options.setExplicitDelims(true);
	    options.setFieldsTerminatedBy('\t');
	    options.setLinesTerminatedBy('\n');
	    int ret = new ImportTool().run(options);
	    return ret;
    }
}
