package com.google.ssadm.rinftechtest.service;

import com.google.ssadm.rinftechtest.domain.PubSubBody;

import java.io.IOException;

public interface FileProcessor {
    void processFile(PubSubBody body) throws InterruptedException, IOException;
}
