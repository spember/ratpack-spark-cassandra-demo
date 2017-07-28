package org.pember.ratpack.sparkdemo.repository

import com.google.inject.Inject
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.spark.api.java.JavaSparkContext
import org.pember.ratpack.sparkdemo.config.CassandraConfig
import org.pember.sparkdemo.shared.pogo.DailyPriceRecord

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow

@Slf4j
class SparkCassandraStockInformationRepository {

    private CassandraConfig cassandraConfig
    private static final String RECORD_TABLE = "daily_price_record"

    @Inject
    SparkCassandraStockInformationRepository(CassandraConfig cassandraConfig) {
        this.cassandraConfig = cassandraConfig
    }

    void save(JavaSparkContext context, List<DailyPriceRecord> records) {
        javaFunctions(context.parallelize(records))
        .writerBuilder(cassandraConfig.keyspace, RECORD_TABLE, mapToRow(DailyPriceRecord.class))
        .saveToCassandra()
    }
}
