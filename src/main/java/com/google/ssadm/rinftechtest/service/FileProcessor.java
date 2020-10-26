package com.google.ssadm.rinftechtest.service;

import com.google.ssadm.rinftechtest.eventpojos.PubSubBody;

public interface FileProcessor {
    void processFile(PubSubBody body);
}
