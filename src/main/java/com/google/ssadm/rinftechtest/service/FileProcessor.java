package com.google.ssadm.rinftechtest.service;

import com.google.ssadm.rinftechtest.entity.PubSubBody;

public interface FileProcessor {
    void processFile(PubSubBody body) throws InterruptedException;
}
