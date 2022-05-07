package com.ygs

import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {

    override def apply(extensions: SparkSessionExtensions): Unit = {
        extensions.injectOptimizerRule { session =>
            MyRule(session)
        }

    }
}
