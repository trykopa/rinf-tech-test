package com.google.ssadm.rinftechtest.service;

import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class AvroRecordToDbAsyncProcessor {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordToDbAsyncProcessor.class);

    @Autowired
    private final BigQuery bqInstance;

    public AvroRecordToDbAsyncProcessor(BigQuery bqInstance) {
        this.bqInstance = bqInstance;
    }

    @Async
    public CompletableFuture <Boolean> asyncProcess(String fileName , String table , String dataSet, Schema schema) throws InterruptedException {
        log.info("Process Avro file {} {}", fileName, table );
        boolean jobDone;
            TableId tableId = TableId.of(dataSet, table);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.newBuilder(tableId, fileName)
                            .setFormatOptions(FormatOptions.avro())
                            .setSchema(schema)
                            .build();
            // Load data from a GCS AVRO file into the table
            Job job = bqInstance.create(JobInfo.of(loadConfig));
            // Blocks until this load table job completes its execution, either failing or succeeding.
            job = job.waitFor();
            if (job.isDone()) {
                log.info("Avro from GCS successfully loaded in a table by Thread {}", Thread.currentThread().getName());
            } else {
                log.debug(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
            jobDone = job.isDone();
        return CompletableFuture.completedFuture(jobDone);
    }
}
