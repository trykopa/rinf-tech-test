package com.google.ssadm.rinftechtest.service;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.ssadm.rinftechtest.eventpojos.PubSubBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class GcsFileProcessor implements FileProcessor{

    private static final Logger log = LoggerFactory.getLogger(GcsFileProcessor.class);

    @Autowired
    private final AvroRecordToDbAsyncProcessor arp;

    @Autowired
    private final RecordToDbProcessor rp;

    public static final String dbName = System.getenv("DATASET");
    public static final String fullTable = System.getenv("TABLEFULL");
    public static final String shortTable = System.getenv("TABLEOPTIONAL");

    public static final Schema shortSchema =
            Schema.of(
                    Field.of("id", StandardSQLTypeName.INT64),
                    Field.of("name", StandardSQLTypeName.STRING));

    public static final Schema fullSchema =
            Schema.of(
                    Field.of("id", StandardSQLTypeName.INT64),
                    Field.of("name", StandardSQLTypeName.STRING),
                    Field.of("phone", StandardSQLTypeName.STRING),
                    Field.of("address", StandardSQLTypeName.STRING));


    public GcsFileProcessor(AvroRecordToDbAsyncProcessor arp , RecordToDbProcessor rp) {
        this.arp = arp;
        this.rp = rp;
    }

    @Override
    public void processFile(PubSubBody body){
        String bucket = body.getMessage().getAttributes().get("bucketId");
        String newFile = body.getMessage().getAttributes().get("objectId");
        String gcsPath = "gs://" + bucket + "/" + newFile;
        Map <String, Schema> schemaMap = Map.of(fullTable, fullSchema, shortTable, shortSchema);
        log.info("Start load data to tables by single thread method");
        schemaMap.forEach((t,s) -> {
            try {
                rp.recordProcessor(gcsPath, t, dbName, s);
            } catch (InterruptedException e) {
                log.error("Error during dingle thread run", e);
                e.printStackTrace();
            }
        });
        log.info("-------------------async method--------------------");
        schemaMap.forEach((t,s) -> {
            try {
                arp.asyncProcess(gcsPath, t, dbName, s);
            } catch (InterruptedException e) {
                e.printStackTrace();
                log.error("Error during async method run", e);
            }
        });
        log.info("Finish load data to tables");
    }
}
