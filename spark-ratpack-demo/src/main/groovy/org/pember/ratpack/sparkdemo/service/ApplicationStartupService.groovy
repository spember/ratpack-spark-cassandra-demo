package org.pember.ratpack.sparkdemo.service

import com.google.inject.Inject
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import ratpack.service.Service
import ratpack.service.StartEvent
import ratpack.service.StopEvent

@Slf4j
@CompileStatic
class ApplicationStartupService implements Service {

    private RecordIngestionService recordIngestionService

    @Inject ApplicationStartupService(RecordIngestionService recordIngestionService) {
        this.recordIngestionService = recordIngestionService
    }

    @Override
    void onStart(StartEvent event) throws Exception {
        log.info("Hey! We started up")
        log.info("Ingesting...")
        recordIngestionService.ingestFromCsv("ticker.csv")

    }

    @Override
    void onStop(StopEvent event) throws Exception {
        log.info("Shutting down")
    }
}
