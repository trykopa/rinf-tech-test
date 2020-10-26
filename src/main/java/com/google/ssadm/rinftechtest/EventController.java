package com.google.ssadm.rinftechtest;

import com.google.ssadm.rinftechtest.eventpojos.PubSubBody;
import com.google.ssadm.rinftechtest.service.FileProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
  public ResponseEntity<String> receiveMessage(
          @RequestBody PubSubBody body, @RequestHeader Map<String, String> headers) {
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
      } catch (NullPointerException npe) {
        npe.printStackTrace();
      }
    } else {
      msg = "TEST FILED, not avro file";
    }
    log.info(msg);
    return new ResponseEntity <>(msg , HttpStatus.OK);
  }
}
