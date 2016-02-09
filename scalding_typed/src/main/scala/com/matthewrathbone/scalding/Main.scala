package main.scala.com.matthewrathbone.scalding

import com.twitter.scalding._
import TDsl._
import cascading.tuple.TupleEntry

case class Transaction(id: Long, productId: Long, userId: Long, purchaseAmount: Double, itemDescription: String)
case class User(id: Long, email: String, language: String, location: String)

class Main (args: Args) extends Job(args) {

  val output = args( "output" )
  
  val input1 : TypedPipe[String] = TypedPipe.from(TextLine(args( "input1" )))
  val input2 : TypedPipe[String] = TypedPipe.from(TextLine(args( "input2" )))
  

  val usersInput : TypedPipe[User] = input1.map{ s: String =>
    val split = s.split("\t")
    User(split(0).toLong, split(1), split(2), split(3))
  }

  val transactionsInput : TypedPipe[Transaction] = input2.map{ s:String =>
    val split = s.split("\t")
    Transaction(split(0).toLong, split(1).toLong, split(2).toLong, split(3).toDouble, split(4))
   }

  val group1 = usersInput.groupBy(_.id)
  val group2 = transactionsInput.groupBy(_.userId)
  
  val joinedBranch =  group2
    .leftJoin(group1) // 'user_id -> 'id, 
    .map{ case (k: Long, (t: Transaction, Some(u: User))) => (t.productId, u.location) }
    .distinct
    .groupBy{ case (productId, location) => productId }
    .size
    .write(TypedTsv[(Long, Long)](output))
}
