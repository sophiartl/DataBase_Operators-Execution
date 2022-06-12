package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, NilTuple, Tuple}


import scala.collection.mutable.Stack

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected(
                              left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                              right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                            ) extends skeleton.Stitch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  var buffer: Stack[LateTuple] = Stack.empty
  var grouped = Map[Long, Iterable[LateTuple]]()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    grouped = left.groupBy(elem => elem.vid)
    right.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] ={
  val r_tuple = right.next()
  if(buffer.isEmpty && r_tuple != NilLateTuple){
    r_tuple match {
      case Some(t) => {
        grouped.filter(elem => elem._1 == t.vid).foreach(arr=> {buffer.push(new LateTuple(t.vid, (arr._2.head.value ++ t.value)))})
        if(buffer.isEmpty) {
          return next()
        } else {

          return Option(buffer.pop())
        }

      }
      case Some(_) => next()

      case NilLateTuple => NilLateTuple
    }
  } else if (!buffer.isEmpty) {
    return Option(buffer.pop())
  } else {
    return NilLateTuple

  }
}


/**
    * @inheritdoc
    */
  override def close(): Unit = {
    grouped = grouped.empty
    right.close()
  }
}
