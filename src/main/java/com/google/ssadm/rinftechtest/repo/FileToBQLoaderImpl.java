package com.google.ssadm.rinftechtest.repo;

import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;

@Repository
@EnableAsync
public class FileToBQLoaderImpl implements FilesToBQLoader {

    public static final String dbName = System.getenv("DATASET");
    public static final String fullTable = System.getenv("TABLEFULL");
    public static final String shortTable = System.getenv("TABLEOPTIONAL");
    private static final Logger log = LoggerFactory.getLogger(FileToBQLoaderImpl.class);


    @Autowired
    private final BigQuery bqInstance;

    public FileToBQLoaderImpl(BigQuery bqInstance) {
        this.bqInstance = bqInstance;
    }

    @Override
    public void fileToBQ(File file, File shortFile) throws IOException, InterruptedException {
        fileWriter(file, fullTable);
        fileWriter(shortFile, shortTable);
    }

    @Async
    public void fileWriter(File file, String tableName) throws IOException, InterruptedException {
        // [START bigquery_load_from_file]
        TableId tableId = TableId.of(dbName, tableName);
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId)
                        .setIgnoreUnknownValues(true)
                        .setFormatOptions(FormatOptions.avro()).build();
        // The location must be specified; other fields can be auto-detected.
        JobId jobId = JobId.newBuilder().setLocation("us").build();
        TableDataWriteChannel writer = bqInstance.writer(jobId, writeChannelConfiguration);
        // Write data to writer
        try (OutputStream stream = Channels.newOutputStream(writer)) {
            Files.copy(file.toPath(), stream);
        }
        // Get load job
        Job job = writer.getJob();
        job = job.waitFor();
        JobStatistics.LoadStatistics stats = job.getStatistics();
        log.info("{} Rows to {} added. Tread-{}",
                stats.getOutputRows().toString(), tableName, Thread.currentThread().getName());
    }

}
