package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
                            input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
                            groupSet: ImmutableBitSet,
                            aggCalls: IndexedSeq[AggregateCall]
                          ) extends skeleton.Aggregate[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](input, groupSet, aggCalls)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */
  var tuples: IndexedSeq[Tuple] = input.toIndexedSeq
  var counter = 0
  var buff:IndexedSeq[IndexedSeq[Any]] = _
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    counter =0
    if(tuples.isEmpty && groupSet.isEmpty){
      buff = IndexedSeq(aggCalls.map(elem=> elem.emptyValue ))
    }else if (tuples.isEmpty){
      buff = IndexedSeq()
    }else if(!groupSet.isEmpty){
      var index = IndexedSeq.range(0,groupSet.length() ,1)
      var gAggr = tuples.groupBy(tuple => index.map(i => tuple(i)))
      buff = gAggr.map{ case (key: IndexedSeq[Any], vals: IndexedSeq[Tuple]) =>
        //recursion
        var combined = for (agg <- aggCalls) yield vals.init.foldLeft(agg.getArgument(vals.last))((acc, t) =>
          agg.reduce(acc, agg.getArgument(t)))
        (key, key ++ combined )
      }.values.toIndexedSeq

    }else{
      var temp = IndexedSeq[Any]()
      buff = IndexedSeq(aggCalls.map(agg => tuples.init.foldLeft(agg.getArgument(tuples.last))((acc,tuple) => agg.reduce(acc,agg.getArgument(tuple)))))
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (counter < buff.length){
      var tuple = buff(counter)
      counter += 1
      return Option(tuple)
    }else{
      return NilTuple
    }
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
