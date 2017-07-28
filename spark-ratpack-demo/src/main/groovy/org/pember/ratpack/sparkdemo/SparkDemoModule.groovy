package org.pember.ratpack.sparkdemo

import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.Scopes
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.pember.ratpack.sparkdemo.config.CassandraConfig
import org.pember.ratpack.sparkdemo.config.SparkConfig
import org.pember.ratpack.sparkdemo.repository.SparkCassandraStockInformationRepository
import org.pember.ratpack.sparkdemo.service.ApiService
import org.pember.ratpack.sparkdemo.service.ApplicationStartupService
import org.pember.ratpack.sparkdemo.service.RecordIngestionService
import org.pember.ratpack.sparkdemo.service.SparkCassandraRecordService

import javax.inject.Singleton

@Slf4j
@CompileStatic
class SparkDemoModule extends AbstractModule {

    @Override
    protected void configure() {
        [
                ApiService.class,
                SparkCassandraStockInformationRepository.class,
                SparkCassandraRecordService.class,
                RecordIngestionService.class,
                ApplicationStartupService.class,
                SparkDemoUrlMapping.class
        ].each{ bind(it).in(Scopes.SINGLETON) }
    }


    @Provides
    @Singleton
    static JavaSparkContext javaSparkContext(SparkConfig sparkConfig, CassandraConfig cassandraConfig) {
        log.info("Connecting to spark with master Url: {}, and cassandra host: {}",
                sparkConfig.getMaster(), cassandraConfig.connection.host)

        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", cassandraConfig.connection.host)
                .set("spark.submit.deployMode", "client")

        JavaSparkContext context = new JavaSparkContext(
                sparkConfig.getMaster(),
                "SparkDemo",
                conf
        )
        //context.addJar("/my/work/dir/spark-shared.jar")
        log.debug("SparkContext created")
        return context
    }
}
