package org.pember.sparkdemo.shared.job

import groovy.transform.CompileStatic
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.pember.sparkdemo.shared.pogo.Company
import org.pember.sparkdemo.shared.pogo.DailyPriceRecord
import org.pember.sparkdemo.shared.pogo.StockOverview
import org.pember.sparkdemo.shared.pogo.StockQuery

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo


@CompileStatic
class SingleStockOverviewJob implements Serializable {
    private static final String STOCK_DATA = "stock_data"
    /**
     * Calculates an Overview of a given stock symbol, for the past 30 days
     *
     *
     * @param context
     * @param stockQuery
     * @return
     */
    public StockOverview execute(JavaSparkContext context, StockQuery stockQuery) {

        /*
            This entire block is executed as one job within spark
         */

        StockOverview overview = new StockOverview()
        overview.setSymbol(stockQuery.getSymbol())

        long startTime = System.currentTimeMillis()
        // grab all the companies
        JavaRDD<Company> companies = javaFunctions(context)
                .cassandraTable(STOCK_DATA, "company", mapRowTo(Company.class))

        JavaRDD<DailyPriceRecord> symbolResults = javaFunctions(context)
                .cassandraTable(STOCK_DATA,
                "daily_price_record",
                mapRowTo(DailyPriceRecord.class)
        )
                .where("symbol = ? and year = ?",
                stockQuery.getSymbol(), stockQuery.getStart().getYear())
                .filter({ DailyPriceRecord record -> (stockQuery.getStart() > record.getRecordedDate()) })
                .cache() as JavaRDD<DailyPriceRecord>
        // cache to avoid repeated queries. Our data set is very small so we won't notice the effect here. But imagine the savings of millions of records


        JavaRDD<Double> rawValues = symbolResults
                .map({it.getValue()})
                .cache()
        // cache() to avoid repeated maps,
        // although this won't actually execute until the
        // first 'reduce' call, below

        // These next two spark calls are Easy!
        // this first call to raw values will calculate the above
        overview.setThirtyDayHigh(rawValues.reduce({Double c1, Double c2 -> c1 > c2 ? c1 : c2}))
        // no need to re-execute rawValues on this second call!
        overview.setThirtyDayLow(rawValues.reduce({Double c1, Double c2 ->c1 < c2 ? c1 : c2}))

        // This spark call is tricky. Why three parameters to build a simple array?
        // (normally I'd create a class instead of using an array, but I feel this is more illustrative of what's happening)
        int[] result = symbolResults.aggregate (
                new int[2], // first argument -> the initial 'zero' object

                {int[] arr, DailyPriceRecord record ->
            // second argument: function to add a value into our aggregater. Here, adding a record result into the aggregator
            // calls to this lambda are done on a single partition
            arr[0] += record.getShareVolume() // running total
            arr[1] += 1 // count
            arr
        },

                {int[] arr1, int[] arr2 -> // second argument
            // finally, the 'spark' moment -> how should spark join the aggregators from each partition into a single partition?
            // in the 'reduce' calls above, those are actually done across partition
            int[] combine = new int[2]
            combine[0] = arr1[0] + arr2[0]
            combine[1] = arr1[1] + arr2[1]
            combine
        })

        println("About to avg " + result[0] +" with " + result[1])
        overview.setThirtyDayAverageVolume((result[0]/result[1]).intValue())



        long endtime = System.currentTimeMillis()
        println("Calculated overview: " + overview.toString ()+ ", elapsed time: " + (endtime-startTime))
        overview
    }

}
