package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
                       input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
                       collation: RelCollation,
                       offset: Option[Int],
                       fetch: Option[Int]
                     ) extends skeleton.Sort[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](input, collation, offset, fetch)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {


  var collations = collation.getFieldCollations().asScala.toList
  var tup1:Comparable[Any] = null
  var tup2:Comparable[Any]= null

  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  var tuples : IndexedSeq[Tuple]= input.execute().transpose.filter(_.last.asInstanceOf[Boolean]).map(_.dropRight(1))

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[Column] = {


    collations.foreach( c=> {
      tuples = tuples.sortWith((elem1, elem2) => {
        sorting(elem1, elem2)
      })
    })
    tuples = tuples.drop(offset.getOrElse(0) )
    tuples = tuples.take(fetch.getOrElse(Int.MaxValue))

    if(tuples.isEmpty) {
      return IndexedSeq()
    }
    val cols = tuples.transpose
    return cols :+ IndexedSeq.fill(cols(0).size)(true)

  }

  def sorting (elem1: Tuple, elem2: Tuple): Boolean = {
    collations.foreach( c=> {
      tup2 = elem2(c.getFieldIndex).asInstanceOf[Comparable[Any]]
      tup1 = elem1(c.getFieldIndex).asInstanceOf[Comparable[Any]]
      var equality = tup1.compareTo(tup2)
      if(equality != 0){
        if (!c.direction.isDescending) {
          return equality < 0
        }else{
          return equality > 0
        }
      } else {

      }
    })
    return false
  }
}