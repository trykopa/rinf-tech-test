package com.google.ssadm.rinftechtest.controller;

import com.google.ssadm.rinftechtest.domain.PubSubBody;
import com.google.ssadm.rinftechtest.service.FileProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
public class EventController {

    @Autowired
    private final FileProcessor fileProcessor;

    public EventController(FileProcessor fileProcessor) {
        this.fileProcessor = fileProcessor;
    }

    private static final Logger log = LoggerFactory.getLogger(EventController.class);


    @RequestMapping(value = "/", method = RequestMethod.POST)
    public ResponseEntity <String> receiveMessage(
            @RequestBody PubSubBody body , @RequestHeader Map <String, String> headers) {
        //Check headers for required fields
        if (headers.get("host") == null && headers.get("content-type") == null && headers.get("from") == null) {
            String msg = "Missing expected headers.";
            log.info(msg);
            return new ResponseEntity <>(msg , HttpStatus.BAD_REQUEST);
        }

        String msg;
        if (body.getMessage().getAttributes().get("objectId").endsWith("avro")) {
            msg = "TEST OK";
            try {
                fileProcessor.processFile(body);
            } catch (NullPointerException | InterruptedException | IOException npe) {
                npe.printStackTrace();
                return new ResponseEntity <>(msg , HttpStatus.BAD_REQUEST);
            }
        } else {
            msg = "TEST FILED, not avro file";
            return new ResponseEntity <>(msg , HttpStatus.BAD_REQUEST);
        }
        log.info(msg);
        return new ResponseEntity <>(msg , HttpStatus.OK);
    }
}
