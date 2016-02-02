package main.scala

import com.twitter.scalding._
import TDsl._
import cascading.tuple.TupleEntry
import com.twitter.scalding.source.TypedText


class Main ( args: Args ) extends Job( args ) {

	val output = args( "output" )
	
	val input1 : TypedPipe[String] = TypedPipe.from(TextLine(args( "input1" )))
	val input2 : TypedPipe[String] = TypedPipe.from(TextLine(args( "input2" )))
	
	val usersInput : TypedPipe[(String, String, String, String)] = input1.map{ s:String =>
      val split = s.split("\t")
      (split( 0 ), split( 1 ), split( 2 ), split( 3 ))
	}
	val group1 = usersInput.groupBy { case (id, email, language, location) => id }

	val transactionsInput : TypedPipe[(String, String, String, String, String)] = input2.map{ s:String =>
      val split = s.split("\t")
      (split( 0 ), split( 1 ), split( 2 ), split( 3 ), split( 4 ))
    }
	val group2 = transactionsInput.groupBy{ case (transaction_id, product_id, user_id, purchase_amount, item_description) => user_id }
	
	val joinedBranch =  group2
			.leftJoin(group1) // 'user_id -> 'id, 
			.map{ case (k, (v0, v1)) => (v0._2, v1.get._4) }
			.distinct
			.groupBy{ case (product_id, location) => product_id }
			.size
			.write(TypedTsv[(String, Long)](output))
}