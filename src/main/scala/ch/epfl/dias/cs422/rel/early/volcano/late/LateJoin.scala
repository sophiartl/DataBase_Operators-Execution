package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Stack

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateJoin(
               left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               condition: RexNode
             ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  var buffer: Stack[LateTuple] = Stack.empty
  var leftKeys= getLeftKeys.toBuffer
  var rightKeys = getRightKeys.toBuffer

  var gB = Map[IndexedSeq[RelOperator.Elem], Iterable[LateTuple]]()


  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    gB = left.groupBy(elem => getLeftKeys.map(i => elem.value(i)))
    right.open()

  }


  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {

    val r_tuple = right.next()
  if(buffer.isEmpty && r_tuple !=NilLateTuple){

    r_tuple match {
      case Some(t) => {
        val gR= r_tuple.groupBy(elem => getRightKeys.map(i => elem.value(i)))
        gB.foreach(arr => if (arr._1 == gR.head._1 ) buffer.push((arr._2 ++ gR.head._2).head))

        buffer.foreach{println}
        if(buffer.isEmpty) {
          return next()
        } else {
          return Option(buffer.pop())
        }

      }
      case Some(_) => next()

      case NilTuple => NilTuple
    }
  } else if (!buffer.isEmpty) {
    return Option(buffer.pop())
  }else {
    return NilTuple

  }
}

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    right.close()
  }
}
