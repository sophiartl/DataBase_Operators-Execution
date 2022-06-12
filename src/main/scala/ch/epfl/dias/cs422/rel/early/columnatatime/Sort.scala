package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.RelCollation

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  var collations = collation.getFieldCollations().asScala.toList
  var tup1:Comparable[Any] = null
  var tup2:Comparable[Any]= null
  val empy = IndexedSeq()


  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
      var data = IndexedSeq[Tuple]()
      data = input.execute().transpose.filter(_.last.asInstanceOf[Boolean]).map(_.dropRight(1))
      collations.foreach( c=> {
        data = data.sortWith((elem1, elem2) => {
          sorting(elem1, elem2)
        })
      })
      data = data.drop(offset.getOrElse(0) )
      data = data.take(fetch.getOrElse(Int.MaxValue))
      if(data.isEmpty) {
        return empy
      }
      val cols = data.transpose
      return  cols.map(col => toHomogeneousColumn(col))  :+ IndexedSeq.fill(cols(0).size)(true)

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
      }
    })
    return false
  }
  }