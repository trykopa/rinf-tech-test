package com.google.ssadm.rinftechtest.repo;

import com.google.ssadm.rinftechtest.domain.Client;

import java.io.IOException;
import java.util.List;

public interface RecordsToBQLoader {
    public void saveRecords(List <Client> clientList) throws IOException;
}
