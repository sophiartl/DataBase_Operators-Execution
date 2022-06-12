package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
                       input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
                       collation: RelCollation,
                       offset: Option[Int],
                       fetch: Option[Int]
                     ) extends skeleton.Sort[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](input, collation, offset, fetch)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */
  var tuples: IndexedSeq[Tuple] = input.toIndexedSeq
  var collations= collation.getFieldCollations()
  var counter= 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    counter =0
    if (fetch.getOrElse(Int.MaxValue) != 0) {
      //from collations
      for (i <- (collations.size() - 1 to 0 by -1)) {
        var f = collations.get(i).getFieldIndex

        tuples = tuples.sortWith((tuple1, tuple2) => {
          if (!collations.get(i).direction.isDescending) {
            RelFieldCollation.compare(
              tuple1(f).asInstanceOf[Comparable[_]],
              tuple2(f).asInstanceOf[Comparable[_]],
              0) < 0
          } else {
            RelFieldCollation.compare(
              tuple1(f).asInstanceOf[Comparable[_]],
              tuple2(f).asInstanceOf[Comparable[_]],
              0) > 0
          }
        })
      }
    } else {
      return

    }

  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] =
    if(counter < tuples.length && tuples.size != 0){
      tuples = tuples.drop(offset.getOrElse(0))
      tuples = tuples.take(fetch.getOrElse(Int.MaxValue))
      var tuple = Option(tuples(counter))
      counter += 1
      return tuple
    }else{
      return NilTuple
    }


  /**
    * @inheritdoc
    */
  override def close(): Unit = {
input.close()
  }



}
