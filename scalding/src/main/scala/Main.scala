package main.scala

import com.twitter.scalding._
import cascading.tuple.TupleEntry


// https://github.com/ANithian/scaldingsample
// https://github.com/twitter/scalding/wiki/SQL-to-Scalding
// bin/hadoop jar java-cascading-1.0-SNAPSHOT-jar-with-dependencies.jar com.twitter.scalding.Tool main.scala.Main --hdfs --input1 input1 --input2 input2 --output output
class Main ( args: Args ) extends Job( args )
  {
  
    val inputFields = 'line
	val users = ( 'id, 'email, 'language, 'location)
  	val transactions = ( 'transaction_id, 'product_id, 'user_id, 'purchase_amount, 'item_description);
	
	val input1 = TextLine( args( "input1" ) )
	val input2 = TextLine( args( "input2" ) )
	val output = Tsv( args( "output" ) )
	
	val usersInput = input1.read.mapTo( inputFields -> users )
    { te: TupleEntry =>
      val split = te.getString( "line" ).split("\t");
      (split( 0 ), split( 1 ), split( 2 ), split( 3 ))
    }

	val transactionsInput = input2.read.mapTo( inputFields -> transactions )
    { te: TupleEntry =>
      val split = te.getString( "line" ).split("\t");
      (split( 0 ), split( 1 ), split( 2 ), split( 3 ), split( 4 ))
    }
	
	val joinedBranch =  transactionsInput
			.joinWithSmaller('user_id -> 'id, usersInput)
			.project('product_id, 'location)
			.unique('product_id, 'location)
			.groupBy('product_id) {group => group.size(Set('location))}
			.write(output)
  }