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

    // 1. Read users and transactions (and part of 2, we're setting the key to userid)
    val users: DList[(Int, User)] = TextInput.fromTextFile(u).map{ line =>
      val split = line.split("\t")

      (split(0).toInt, User(split(0).toInt, split(1), split(2), split(3)))
    }
    val transactions: DList[(Int, Transaction)] = TextInput.fromTextFile(t).map { line =>
      val split = line.split("\t")
      (split(2).toInt, Transaction(split(0).toInt, split(1).toInt, split(2).toInt, split(3)))
    }

    // 2. join users to transactions based on userid
    val relation = Relational(transactions)

    val joined: DList[(Int, (Transaction, Option[User]))] = relation.joinLeft(users)

    // 3. transform so we have a Dlist of (productid, Option[location])
    // 4. group by key to get (productid, Iterable[location])
    val grouped: DList[(Int, Iterable[Option[String]])] = joined.map{ case(key: Int, (t: Transaction, oU: Option[User])) =>
      (t.productId, oU.map(_.location))
    }.groupByKey

    // 5. find the distinct number of locations for each product. This step was hard to work out.
    val flattened: DList[(Int, Iterable[Long])] = grouped.map{ case(product: Int, locations: Iterable[Option[String]]) => 
      var last: Option[String] = None
      // this works becase we know the values are sorted.
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
    // write to the output directoryp
    persist(toTextFile(result, out))
  }
}