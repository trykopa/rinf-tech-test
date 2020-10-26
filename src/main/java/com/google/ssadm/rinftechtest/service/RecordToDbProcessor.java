package com.google.ssadm.rinftechtest.service;

import com.google.cloud.bigquery.Schema;

public interface RecordToDbProcessor {
    void recordProcessor(String filename, String table, String dataSet, Schema schema) throws InterruptedException;
}
