package org.pember.ratpack.sparkdemo.service

import com.google.inject.Inject
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.pember.sparkdemo.shared.pogo.DailyPriceRecord

import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Collectors

@Slf4j
@CompileStatic
class RecordIngestionService {
    private SparkCassandraRecordService recordService

    @Inject RecordIngestionService(SparkCassandraRecordService recordService) {
        this.recordService = recordService
    }

    void ingestFromCsv(String filename) {
        try {
            List<DailyPriceRecord> lines
            URI filePath = ClassLoader.getSystemResource(filename).toURI()
            log.info("URI = ${filePath}")
            lines = Files.lines(Paths.get(filePath))
            .map({DailyPriceRecord.buildFromFileRow(it)})
            .collect(Collectors.toList())

            log.info("Received ${lines.size()} lines")
            recordService.save(lines)
        } catch(NullPointerException | URISyntaxException | IOException e) {
            log.error("Could not find file {}", filename, e);
            throw e;
        }
    }
}
