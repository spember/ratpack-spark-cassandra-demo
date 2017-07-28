package org.pember.sparkdemo.shared.job

import groovy.transform.CompileStatic
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.pember.sparkdemo.shared.pogo.DailyPriceRecord
import org.pember.sparkdemo.shared.pogo.StockOverview

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo

@CompileStatic
class ScratchWorkJob {


//    StockOverview execute(JavaSparkContext context, String query) {
//
//        JavaRDD<DailyPriceRecord> symbolResults = grabRawRecords(context, query)
//
//        JavaRDD<Double> symbolValues = symbolResults
//                .map({record -> record.getValue()})
//
//        Double maxValue = symbolValues.reduce(
//                {value1, value2 -> value1 > value2 ? value1 : value2}
//        )
//
//        println("Max value is " + maxValue)
//        null
//    }
////
////
//    private JavaRDD<DailyPriceRecord> grabRawRecords(JavaSparkContext context, String symbol) {
//
//        javaFunctions(context)
//                .cassandraTable("stock_data", "daily_price_record", mapRowTo(DailyPriceRecord.class))
//                //.where("symbol = ? and year = ? and month >= ? and day >= ?", symbol, target.get(Calendar.YEAR), target.get(Calendar.MONTH), target.get(Calendar.DAY_OF_MONTH))
//                .where("symbol = ? and year = ? and day >= ?", symbol, 2017, target.get(Calendar.DAY_OF_MONTH))
//    }
}
