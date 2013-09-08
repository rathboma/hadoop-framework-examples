// copyright Matthew Rathbone 2013

package com.matthewrathbone.example

import com.nicta.scoobi.Scoobi._

import Reduction.Sum
import com.nicta.scoobi.lib.Relational
import com.nicta.scoobi.core.WireFormat
import com.nicta.scoobi.io.text.TextInput

object Example extends ScoobiApp {

  case class User(id: Int, email: String, lang: String, location: String)
  case class Transaction(id: Int, productId: Int, userId: Int, itemDescription: String)

  def run() {
    val u = args(0)
    val t = args(1)
    val out = args(2)

    implicit val tformat = WireFormat.mkCaseWireFormat(Transaction, Transaction.unapply _)
    implicit val uformat = WireFormat.mkCaseWireFormat(User, User.unapply _)

    val users: DList[(Int, User)] = TextInput.fromTextFile(u).map{ line =>
      val split = line.split("\t")
      // we have to set this up as a K,V for the join later on
      (split(0).toInt, User(split(0).toInt, split(1), split(2), split(3)))
    }
    val transactions: DList[(Int, Transaction)] = TextInput.fromTextFile(t).map { line =>
      val split = line.split("\t")
      (split(2).toInt, Transaction(split(0).toInt, split(1).toInt, split(2).toInt, split(3)))
    }
    val outputDirectory = args(2)

    val relation = Relational(transactions)

    val joined: DList[(Int, (Transaction, Option[User]))] = relation.joinLeft(users)

    val grouped: DList[(Int, Iterable[Option[String]])] = joined.map{ case(key: Int, (t: Transaction, oU: Option[User])) =>
      (t.productId, oU.map(_.location))
    }.groupByKey

    val flattened: DList[(Int, Iterable[Long])] = grouped.map{ case(product: Int, locations: Iterable[Option[String]]) => 
      var last: Option[String] = None
      val distinctValues = locations.map{l =>
        val result = (last, l) match {
          case (Some(a), Some(b)) if (a == b) => 0l
          case (_, Some(b)) => 1l
          case other => 0
        }
        last = l
        result
      }      
      (product, distinctValues)
    }
    val result = flattened.combine(Sum.long)
    persist(toTextFile(result, out))
  }
}