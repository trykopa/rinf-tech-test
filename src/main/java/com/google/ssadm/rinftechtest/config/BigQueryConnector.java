package com.google.ssadm.rinftechtest.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.core.GcpProjectIdProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Provides client objects for interfacing with BigQuery.
 */

@Configuration
public class BigQueryConnector {

    @Autowired
    CredentialsProvider credentialsProvider;

    @Autowired
    GcpProjectIdProvider projectIdProvider;

    @Bean
    public BigQuery getInstance() throws IOException {
        // projectId needs to be set explicitly even if it's there in the json key!!
        return BigQueryOptions.newBuilder().setProjectId(projectIdProvider.getProjectId())
                .setCredentials(credentialsProvider.getCredentials()).build().getService();
    }

}
