package com.ygs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Add, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
        case Add(left, right, failOnError) if right.isInstanceOf[Literal] &&
            right.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 =>
            logInfo("MyRule 优化规则生效")
            right
    }

}
