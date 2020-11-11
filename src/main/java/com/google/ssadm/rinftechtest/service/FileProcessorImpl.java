package com.google.ssadm.rinftechtest.service;

import com.google.cloud.ReadChannel;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.ssadm.rinftechtest.domain.Client;
import com.google.ssadm.rinftechtest.domain.PubSubBody;
import com.google.ssadm.rinftechtest.repo.FilesToBQLoader;
import com.google.ssadm.rinftechtest.repo.RecordsToBQLoader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class FileProcessorImpl implements FileProcessor {

    @Value("${spring.cloud.gcp.project-id}")
    private String PROJECT_ID;
    private static final Logger log = LoggerFactory.getLogger(FileProcessorImpl.class);
    private static final String FILE_NAME_FULL = "temp.avro";
    private static final String FILE_NAME_SHORT = "temp_short.avro";


    @Autowired
    private final RecordsToBQLoader recordsToBQLoader;

    @Autowired
    private final FilesToBQLoader filesToBQLoader;

    public FileProcessorImpl(RecordsToBQLoader recordsToBQLoader , FilesToBQLoader filesToBQLoader) {
        this.recordsToBQLoader = recordsToBQLoader;
        this.filesToBQLoader = filesToBQLoader;
    }


    @Override
    public void processFile(PubSubBody body) throws IOException, BigQueryException, InterruptedException {
        String bucket = body.getMessage().getAttributes().get("bucketId");
        String newFile = body.getMessage().getAttributes().get("objectId");

        Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
        Blob blob = storage.get(bucket , newFile);
        ReadChannel readChannel = blob.reader();
        File file = new File("/tmp/" + FILE_NAME_FULL);
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        fileOutputStream.getChannel().transferFrom(readChannel , 0 , Long.MAX_VALUE);
        fileOutputStream.close();

        List <Client> clientList = getClientList(file);
        List <Client> shortClientList = newClientList(clientList);
        writeShortFileClient(shortClientList);
        File shortFile = new File("/tmp/" + FILE_NAME_SHORT);

        //Streaming is not available via the free tier.
        //recordsToBQLoader.saveRecords(clientList);
        //DML also available so i need to work only with avro file to BQ methods

        filesToBQLoader.fileToBQ(file , shortFile);

        //clean workspace

        if (file.exists() && shortFile.exists()){
            log.info("File delete {} and {}" , file.getName() , shortFile.getName());
            file.deleteOnExit();
            shortFile.deleteOnExit();
        }
    }

    public List <Client> getClientList(File file) throws IOException {
        DatumReader <Client> clientDatumReader = new SpecificDatumReader <>(Client.class);
        DataFileReader <Client> clientDataFileReader = new DataFileReader <>(file , clientDatumReader);
        List <Client> clientList = new ArrayList <>();
        while (clientDataFileReader.hasNext()) {
            Client client = clientDataFileReader.next();
            clientList.add(client);
        }
        return clientList;
    }

    public void writeShortFileClient(List <Client> clientList) throws IOException {
        DatumWriter <Client> clientDatumWriter = new SpecificDatumWriter <>(Client.class);
        DataFileWriter <Client> dataFileWriter = new DataFileWriter <>(clientDatumWriter);
        dataFileWriter.create(clientList.get(0).getSchema() , new File("/tmp/" + FILE_NAME_SHORT));
        for (Client client : clientList) {
            dataFileWriter.append(client);
        }
        dataFileWriter.close();
    }

    public List <Client> newClientList(List <Client> clientList) {
        return clientList.stream()
                .map(FileProcessorImpl :: setToNullAllOptionalFields)
                .collect(Collectors.toList());
    }

    private static Client setToNullAllOptionalFields(Client client) {
        client.getSchema().getFields().stream()
                .filter(field -> field.schema().isNullable())
                .forEach(field -> client.put(field.name() , null));
        return client;
    }
}
