package org.pember.ratpack.sparkdemo.service

import com.google.inject.Inject
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.spark.api.java.JavaSparkContext
import org.pember.ratpack.sparkdemo.repository.SparkCassandraStockInformationRepository
import org.pember.sparkdemo.shared.pogo.DailyPriceRecord

/**
 * Effectively a wrapper around the repository, it's main purpose is to have the {@link JavaSparkContext} autowired into it, then passed at
 * run time to the underlying repository for querying.
 * The reason for this? The service wrapping your spark calls appears to be serialized and transferred along with the code that actually gets executed
 * Spring @Autowired doesn't seem to play nice with this; the actual class likely gets transferred and executed outside
 * of the Spring lifecycle, so the field would not be populated.
 *
 */
@Slf4j
@CompileStatic
class SparkCassandraRecordService {
    private JavaSparkContext javaSparkContext
    private SparkCassandraStockInformationRepository repository

    @Inject
    SparkCassandraRecordService(JavaSparkContext javaSparkContext, SparkCassandraStockInformationRepository repository) {
        this.javaSparkContext = javaSparkContext
        this.repository = repository
    }

    void save(List<DailyPriceRecord> records) {
        repository.save(javaSparkContext, records);
    }
}
