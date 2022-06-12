package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchProjectRuleSkeleton
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rex.RexNode

/**
  * RelRule (optimization rule) that finds a reconstruct operator that stitches
  * a new expression (projection over one column) to the late materialized tuple
  * and transforms stitching into a fetch operator with projections.
  *
  * To use this rule: LazyFetchProjectRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchProjectRule protected (config: RelRule.Config)
  extends LazyFetchProjectRuleSkeleton(
    config
  ) {

  override def onMatchHelper(call: RelOptRuleCall): RelNode = {
    val filter: RelNode = call.rel(1)
    val fetch_type: LateColumnScan = call.rel(3)
    val column = fetch_type.getColumn
    val projects: Project = call.rel(2)
    val hi = Some(projects.getProjects())
    val class_ = classOf[LogicalFetch]
    var fetch = LogicalFetch.create(filter, fetch_type.deriveRowType(), column, hi, class_)
    return fetch

  }
}

  object LazyFetchProjectRule {

    /**
      * Instance for a [[LazyFetchProjectRule]]
      */
    val INSTANCE = new LazyFetchProjectRule(
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
                b2.operand(classOf[LogicalProject])
                  // of any inputs
                  .oneInput(
                    b3 =>
                      b3.operand(classOf[LateColumnScan])
                        .anyInputs()
                  )
            )
        )
    )

}
