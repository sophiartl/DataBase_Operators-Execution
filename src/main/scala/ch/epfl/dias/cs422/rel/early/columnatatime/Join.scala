package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
            left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
            right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
            condition: RexNode
          ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */
  var left_input = left.execute()
  var right_input = right.execute()
  val empy = IndexedSeq()
  var res: IndexedSeq[Tuple] = IndexedSeq()

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {

    val leftTuples = left_input.transpose.filter(elem => elem.last.asInstanceOf[Boolean]).map(e =>e.dropRight(1))
    if(leftTuples.isEmpty){
      return empy
    }

    val r_tuple = right_input.transpose.filter(elem=> elem.last.asInstanceOf[Boolean]).map(e=> e.dropRight(1))
    var leftKeys = getLeftKeys
    var group = leftTuples.groupBy(tuple => leftKeys.map(tuple(_)))
    r_tuple.foreach(elem => {
      var key = getRightKeys.map(e => elem(e))
      if(group.contains(key)){
        res = res ++ group.getOrElse(key,null)
          .map(left => (left ++ elem))
      }
    })
    if(res.isEmpty){
      return empy
    }
    var cols = res.transpose
    var fill =IndexedSeq.fill(cols(0).size)(true)
    return cols.map(toHomogeneousColumn(_)) :+ fill


  }
}
