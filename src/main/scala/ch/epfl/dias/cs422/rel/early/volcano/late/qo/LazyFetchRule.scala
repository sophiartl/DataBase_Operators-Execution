package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.LogicalStitch
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchRuleSkeleton
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch}
import org.apache.calcite.rel.RelNode

/**
  * RelRule (optimization rule) that finds an operator that stitches a new column
  * to the late materialized tuple and transforms stitching into a fetch operator.
  *
  * To use this rule: LazyFetchRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchRule protected (config: RelRule.Config)
  extends LazyFetchRuleSkeleton(
    config
  ) {
  override def onMatchHelper(call: RelOptRuleCall): RelNode = {
    val filter: RelNode = call.rel(1)
    val fetch_type: LateColumnScan = call.rel(2)
    val column = fetch_type.getColumn
    val projects = None
    val class_ = classOf[LogicalFetch]
    var fetch = LogicalFetch.create(filter, fetch_type.deriveRowType(), column, projects, class_)
    return fetch

  }

}
  object LazyFetchRule {

    /**
      * Instance for a [[LazyFetchRule]]
      */
    val INSTANCE = new LazyFetchRule(
      // By default, get an empty configuration
      RelRule.Config.EMPTY
        // and match:
        .withOperandSupplier((b: RelRule.OperandBuilder) =>
          // A node of class classOf[LogicalStitch]
          b.operand(classOf[LogicalStitch])
            // that has inputs:
            .inputs(
              b1 =>
                // A node that is a LateColumnScan
                b1.operand(classOf[RelNode])
                  // of any inputs
                  .anyInputs(),
              b2 =>
                // A node that is a LateColumnScan
                b2.operand(classOf[LateColumnScan])
                  // of any inputs
                  .anyInputs()
            )
        )
    )
  }

