package org.pember.ratpack.sparkdemo

import com.google.inject.Inject
import org.apache.spark.api.java.JavaSparkContext
import org.pember.sparkdemo.shared.job.AllStocksOverviewJob
import org.pember.sparkdemo.shared.job.SingleStockOverviewJob
import org.pember.sparkdemo.shared.pogo.StockQuery
import ratpack.groovy.handling.GroovyChainAction
import ratpack.handling.Context

import java.time.LocalDate

import static ratpack.jackson.Jackson.json


class SparkDemoUrlMapping extends GroovyChainAction {

    private JavaSparkContext javaSparkContext

    @Inject SparkDemoUrlMapping(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext
    }

    @Override
    void execute() throws Exception {

        path () { ctx ->
            byMethod {
                get {
                    ctx.render(json((new AllStocksOverviewJob()).execute(javaSparkContext,
                            new StockQuery(start: LocalDate.now().minusDays(30)))))
                }
            }

        }
        path (":stockId") {Context ctx ->
            String stockId = ctx.getPathTokens().get("stockId")
            ctx.render(json((new SingleStockOverviewJob()).execute(javaSparkContext, new StockQuery(symbol: stockId, start: LocalDate.now().minusDays(30)))))
        }

    }
}
