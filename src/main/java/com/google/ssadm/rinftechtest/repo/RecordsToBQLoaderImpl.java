package com.google.ssadm.rinftechtest.repo;

import com.google.cloud.bigquery.*;
import com.google.ssadm.rinftechtest.domain.Client;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
@EnableAsync
public class RecordsToBQLoaderImpl implements RecordsToBQLoader {

    public static final String dbName = System.getenv("DATASET");
    public static final String fullTable = System.getenv("TABLEFULL");
    public static final String shortTable = System.getenv("TABLEOPTIONAL");

    @Autowired
    private final BigQuery bqInstance;

    public RecordsToBQLoaderImpl(BigQuery bqInstance) {
        this.bqInstance = bqInstance;
    }

    private static final Logger log = LoggerFactory.getLogger(RecordsToBQLoaderImpl.class);


    @Override
    public void saveRecords(List <Client> clientList) throws IOException {
        if (tableExists(dbName , fullTable) && tableExists(dbName , shortTable)) {

            List <InsertAllRequest.RowToInsert> rowToInsertContentFull = new ArrayList <>();
            List <InsertAllRequest.RowToInsert> rowToInsertContentOptional = new ArrayList <>();

            for (Client client : clientList) {
                Map <String, Object> rowContentFull = new HashMap <>();
                Map <String, Object> rowContentNotOptional = new HashMap <>();

                List <Schema.Field> fields = client.getSchema().getFields();
                for (Schema.Field field : fields) {
                    rowContentFull.put(String.valueOf(field.name()) , client.get(String.valueOf(field.name())));
                    if (!field.schema().isNullable()) {
                        rowContentNotOptional.put(field.name() , client.get(field.name()));
                    }
                }
                rowToInsertContentFull.add(InsertAllRequest.RowToInsert.of(rowContentFull));
                rowToInsertContentOptional.add(InsertAllRequest.RowToInsert.of(rowContentNotOptional));
            }
            //Streaming is not available via the free tier.
            tableInsertRowsWithoutRowIds(dbName , fullTable , rowToInsertContentFull);
            tableInsertRowsWithoutRowIds(dbName , shortTable , rowToInsertContentOptional);
        } else {
            log.error("Table or tables not found");
            throw new IOException("Table or tables not found");
        }
    }


    //Streaming is not available via the free tier.
    @Async
    public void tableInsertRowsWithoutRowIds(
            String datasetName , String tableName , List <InsertAllRequest.RowToInsert> rows) {
        Thread.currentThread().setName(datasetName + "-" + tableName);
        try {
            TableId tableId = TableId.of(datasetName , tableName);
            log.info(String.valueOf(rows));
            InsertAllResponse response =
                    bqInstance.insertAll(InsertAllRequest.newBuilder(tableId).setRows(rows).build());

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    log.error("Response error: \n" + entry.getValue() + response.getInsertErrors());
                }
            }
            log.info("Rows successfully inserted into table without row ids " + Thread.currentThread().getName());
        } catch (BigQueryException e) {
            log.error("Insert operation not performed \n" , e);
        }
    }


    public boolean tableExists(String datasetName , String tableName) {
        try {
            Table table = bqInstance.getTable(TableId.of(datasetName , tableName));
            if (table.exists()) {
                log.info("Table already exist");
                return true;
            } else {
                log.error("Table not found");
                return false;
            }
        } catch (BigQueryException e) {
            log.error("Table not found. \n" , e);
            return false;
        }
    }
}
