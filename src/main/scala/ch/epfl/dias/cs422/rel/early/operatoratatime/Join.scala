package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
            left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
            right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
            condition: RexNode
          ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  var left_input = left.execute()
  var right_input = right.execute()
  var empy = IndexedSeq()
  /**
    * return only the active tuples
    * @inheritdoc
    */
  override def execute(): IndexedSeq[Column] = {

    val leftTuples = left_input.transpose.filter(elem => elem.last.asInstanceOf[Boolean]).map(e => e.dropRight(1))
    if(leftTuples.isEmpty){
      return empy
    }
    val tuples_r = right_input.transpose.filter(elem => elem.last.asInstanceOf[Boolean]).map(e => e.dropRight(1))
    val leftKeys = getLeftKeys
    val gB = leftTuples.groupBy(tuple => leftKeys.map(tuple(_)))
    var res: IndexedSeq[Tuple] = IndexedSeq()
   tuples_r.foreach(elem => {
      var key = getRightKeys.map(e => elem(e))
      if(gB.contains(key)){
        res = res ++ gB.getOrElse(key,null)
          .map(left => (left ++ elem))
      }
    })
    if(!res.isEmpty){
      var cols = res.transpose
      var fill =IndexedSeq.fill(cols(0).size)(true)
      return cols :+ fill
    } else {
      return empy

    }

  }


}
