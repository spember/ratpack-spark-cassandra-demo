package org.pember.sparkdemo.shared.job

import groovy.transform.CompileStatic
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.pember.sparkdemo.shared.pogo.Company
import org.pember.sparkdemo.shared.pogo.DailyPriceRecord
import org.pember.sparkdemo.shared.pogo.RecordValueContext
import org.pember.sparkdemo.shared.pogo.StockOverview
import org.pember.sparkdemo.shared.pogo.StockQuery
import scala.Tuple2

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo

@CompileStatic
class AllStocksOverviewJob implements Serializable {
    private static final String STOCK_DATA = "stock_data"

    List<StockOverview> execute(JavaSparkContext context, StockQuery stockQuery) {
        println("My context is ${context}")
        JavaPairRDD<String, Company> companies = javaFunctions(context)
                .cassandraTable(STOCK_DATA, "company", mapRowTo(Company.class))
        // now convert the list into a key-based pairing
                .mapToPair({Company company -> new Tuple2<>(company.getSymbol(), company)})


        JavaRDD<DailyPriceRecord> symbolResults = javaFunctions(context)
                .cassandraTable("stock_data", "daily_price_record", mapRowTo(DailyPriceRecord.class)
        )
        // demo of filter operation, although should really reduce size by limiting cassandra query
        // but I can't because the daily price record table is saved with symbol as partition key
        // this is an excellent example of c* encouraging dupe tables: I could create a record_by_date with date
        // as the partition key. identical other columns, though
                .filter({DailyPriceRecord record -> (stockQuery.getStart() > record.getRecordedDate()) })
                .cache() as JavaRDD<DailyPriceRecord>

        // convert my original records into a key-based pair as well
        JavaPairRDD<String, DailyPriceRecord> keyedRecords = symbolResults.mapToPair({DailyPriceRecord record -> new Tuple2<>(record.getSymbol(), record)})

        // many spark functions require a JavaPairRDD. Specifically, joining, although it also allows for easy aggregation by keys
        JavaPairRDD<String, RecordValueContext> values = keyedRecords.join(companies)
            .mapValues({ Tuple2 joinedTuple ->
                DailyPriceRecord current = joinedTuple._1() as DailyPriceRecord
                Company company = joinedTuple._2() as Company
                new RecordValueContext(current.getSymbol(), company.getName(), current.getValue(), current.getShareVolume())
            })
        // or: .mapValues(joinedTuple -> new RecordValueContext(joinedTuple._1.getSymbol(), joinedTuple._2.getName(), joinedTuple._1.getValue()))
                .cache()
        // call cache to prevent re-processing of the previous operations when the next several operations are executed

        // Next we'll perform several breakdowns of the daily values, but keep the results in rdds for later joining
        // this first call to raw values will calculate the above
        // the resulting pair will be the symbol as key with the context containing the highest value
        JavaPairRDD<String, RecordValueContext> thirtyDayHighs =
                values.reduceByKey({RecordValueContext context1, RecordValueContext context2 ->
                    context1.getValue() > context2.getValue() ? context1 : context2
                })

        // no need to re-execute 'values' on this second call!
        JavaPairRDD<String, RecordValueContext> thirtyDayLows =
                values.reduceByKey({RecordValueContext context1, RecordValueContext context2 ->
                    context1.getValue() < context2.getValue() ? context1 : context2
                })

        // This spark call is tricky. Why three parameters to build a simple array?
        // (normally I'd create a class instead of using an array, but I feel this is more illustrative of what's happening)
        JavaPairRDD<String, int[]> countAndTotalValue = values.aggregateByKey(
                // first argument -> the initial 'zero' object, aggregator
                new int[2],
                // second argument: function to add a value into our aggregater. Here, adding a record result into the aggregator
                // calls to this lambda are done on a single partition
                {int[] arr, RecordValueContext record ->
            // build running total for upcoming reduce / aggregation operation
            arr[0] += record.getShareVolume() // running total
            arr[1] += 1 // count
            arr
        },
                // finally, the 'spark' moment -> how should spark join the aggregators from each partition into a single partition?
                // in the 'reduce' calls above, those are actually done across partition
                {int[] arr1, int[] arr2 ->
            int[] combined = new int[2]
            combined[0] = arr1[0] + arr2[0]
            combined[1] = arr1[1] + arr2[1]
            combined
        }
        )

        // JavaPairRDD<String, RecordValueContext> thirtyDayHighs
        // JavaPairRDD<String, RecordValueContext> thirtyDayLows
        // now join them back up! .join() will create a new PairRDD of two contexts,
        // matched on the String keys
        JavaRDD<StockOverview> finalResults = thirtyDayHighs.join(thirtyDayLows)
                // at this step we've joined the thirtyDayHigh with the thirtyDayLow
                .mapValues({Tuple2<RecordValueContext, RecordValueContext> highLowTuple ->
                    // for each symbol and high/low pair, create a presentation class
                    // this could be cleaner, say with a Builder or constructor
                    StockOverview overview = new StockOverview()
                    overview.setCompanyName(highLowTuple._1.getCompanyName())
                    overview.setSymbol(highLowTuple._1.getSymbol())
                    overview.setThirtyDayHigh(highLowTuple._1.getValue())
                    overview.setThirtyDayLow(highLowTuple._2().getValue())
                    overview
                })
                // at this step we have JavaPairRDD<String, StockOverview>. join again!
                .join(countAndTotalValue)
                .mapValues({Tuple2<StockOverview, int[]> joinedTuple ->
                    StockOverview overview = joinedTuple._1
                    // this int arr has share volume in the 0 index and total count in the 1 index.
                    // Should probably make a class but eh this is a demo, don't judge me
                    int[] arr = joinedTuple._2
                    overview.setThirtyDayAverageVolume((arr[0]/arr[1]).intValue())
                    overview
                })
                //extract just the values ignore the key
                .map({Tuple2<String, StockOverview> tuple -> tuple._2})

        finalResults.collect()
    }
}
