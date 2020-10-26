package com.google.ssadm.rinftechtest.service;

import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class AvroRecordToDbProcessor implements RecordToDbProcessor {

    private static final Logger log = LoggerFactory.getLogger(AvroRecordToDbProcessor.class);

    @Autowired
    private final BigQuery bqInstance;

    public AvroRecordToDbProcessor(@Qualifier("getInstance") BigQuery bqInstance) {
        this.bqInstance = bqInstance;
    }

    @Override
    public void recordProcessor(String fileName , String table , String dataSet, Schema schema) {
        try {
            log.info("Process Avro file {} {}", fileName, table );
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
                log.info("Avro from GCS successfully loaded in a table by single thread");
            } else {
                log.debug(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            log.error("Column not added during load append \n" , e);
        }
    }


}
