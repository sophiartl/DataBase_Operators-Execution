package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  private var prog = getRowType.getFieldList.asScala.map(_ => 0)

  var counter = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    println("SCAN ")
    counter =  0

  }


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (counter < scannable.getRowCount){
      var rs = scannable.asInstanceOf[RowStore]
      var tuple = rs.getRow(counter)
      counter = counter + 1
      return Option(tuple)

    } else {
      return NilTuple
    }



  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}
}
