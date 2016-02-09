package main.scala.com.matthewrathbone.scalding

import com.twitter.scalding._
import TDsl._
import cascading.tuple.TupleEntry


class Main ( args: Args ) extends Job( args ) {

	val output = args( "output" )
	
	val input1 : TypedPipe[String] = TypedPipe.from(TextLine(args( "input1" )))
	val input2 : TypedPipe[String] = TypedPipe.from(TextLine(args( "input2" )))
	
	case class User(id: Long, email: String, language: String, location: String){}
	val usersInput : TypedPipe[User] = input1.map{ s:String =>
      val split = s.split("\t")
      User(split( 0 ).toLong, split( 1 ), split( 2 ), split( 3 ))
	}
	val usersFields : TypedPipe[(Long, String, String, String)] = usersInput.map {u => (u.id, u.email, u.language, u.location) }
	val group1 = usersFields.groupBy { case (id, email, language, location) => id }
	
	case class Transaction(id: Long, productId: Long, userId: Long, purchaseAmount: Double, itemDescription: String){}
	val transactionsInput : TypedPipe[Transaction] = input2.map{ s:String =>
      val split = s.split("\t")
      Transaction(split( 0 ).toLong, split( 1 ).toLong, split( 2 ).toLong, split( 3 ).toDouble, split( 4 ))
    }

	val transactionsFields : TypedPipe[(Long, Long, Long, Double, String)] = transactionsInput.map { t => (t.id, t.productId, t.userId, t.purchaseAmount, t.itemDescription) }
	val group2 = transactionsFields.groupBy{ case (id, productId, userId, purchaseAmount, itemDescription) => userId }
	
	val joinedBranch =  group2
			.leftJoin(group1) // 'user_id -> 'id, 
			.map{ case (k, (v0, v1)) => (v0._2, v1.get._4) }
			.distinct
			.groupBy{ case (product_id, location) => product_id }
			.size
			.write(TypedTsv[(Long, Long)](output))
}
