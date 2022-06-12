package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
                         input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
                         condition: RexNode
                       ) extends skeleton.Filter[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](input, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }
  var res = IndexedSeq[Column]()
  var tuples = input.toIndexedSeq.transpose

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[Column] = {

    tuples.foreach(elem => {
      var tuple = elem.dropRight(1)
      if (predicate(tuple)){
        res = res :+ elem
      }else {
        res = res :+ (tuple :+ false)
      }
    })
    return res.transpose
  }
}
