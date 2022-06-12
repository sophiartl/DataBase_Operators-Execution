package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Aggregate protected (
                            input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
                            groupSet: ImmutableBitSet,
                            aggCalls: IndexedSeq[AggregateCall]
                          ) extends skeleton.Aggregate[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](input, groupSet, aggCalls)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */
  var tuples: IndexedSeq[Tuple] = IndexedSeq()
  var cols = input.toIndexedSeq
  /**
    * Will generate only active tuples
    *
    * @inheritdoc
    */
  override def execute(): IndexedSeq[Column] = {

    var agg: IndexedSeq[Column] = IndexedSeq()
    if (cols.isEmpty && groupSet.isEmpty) {
      aggCalls.foreach(elem => agg :+ elem.emptyValue )
      return agg :+ IndexedSeq(true)
    } else if (cols.isEmpty) {
      return IndexedSeq()
    } else if (!groupSet.isEmpty) {
      val index = IndexedSeq.range(0,groupSet.length() ,1)
      tuples = cols.transpose.filter(e => e.last.asInstanceOf[Boolean])
      if(tuples.isEmpty) {
        return IndexedSeq() :+ IndexedSeq(true)
      }
      tuples = tuples.map(elem => elem.dropRight(1))
      var gB = tuples.groupBy(tuple => index.map(i => tuple(i)))
      agg = gB.map { case (key: IndexedSeq[Any], vals: IndexedSeq[Tuple]) =>
        var combined = for (agg <- aggCalls) yield vals.init.foldLeft(agg.getArgument(vals.last))((acc, t) =>
          agg.reduce(acc, agg.getArgument(t)))
        (key, key ++ combined )
      }.values.toIndexedSeq
      return agg.transpose :+ IndexedSeq.fill(agg.size)(true)
    }else{

      tuples = cols.transpose.filter(elem => elem.last.asInstanceOf[Boolean])
      tuples = tuples.map(elem => elem.dropRight(1))
      if (tuples.isEmpty){
        return agg :+ IndexedSeq(true)
      }
      val result = aggCalls.map(elem => {
        tuples.init.foldLeft(elem.getArgument(tuples.last))((acc,t) => elem.reduce(acc,elem.getArgument(t)))
      })
      var res = result.map(elem => IndexedSeq(elem))
      val fill =  IndexedSeq.fill(res.head.size)(true)
      return res :+ fill

    }
  }


}
