package ch.epfl.dias.cs422.rel.early.volcano


import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{ NilTuple, Tuple}
import org.apache.calcite.rex.RexNode
import scala.collection.mutable.Stack


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
            left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
            right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
            condition: RexNode
          ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  var buff: Stack[Tuple] = Stack ()
  var right_tups: IndexedSeq[Tuple]= right.toIndexedSeq
  var left_tups:IndexedSeq[Tuple] = left.toIndexedSeq
  var gB : Map[Int, IndexedSeq[Tuple]] = _



  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    if(left_tups.isEmpty){
      return
    }
    gB= left_tups.groupBy(elem => getLeftKeys.map(i => elem(i)).hashCode())

    right_tups.foreach(elem =>{
      var keys = getRightKeys.map(i => elem(i)).hashCode()
      if(gB.contains(keys)){
        gB.getOrElse(keys,null).foreach(left => buff.push((left ++ elem)))
      }
    })


  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (!buff.isEmpty){
      val tuple = buff.pop()
      return Some(tuple)
    }else{
      return NilTuple
    }
  }



  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    right.close()
    left.close()
  }


}

