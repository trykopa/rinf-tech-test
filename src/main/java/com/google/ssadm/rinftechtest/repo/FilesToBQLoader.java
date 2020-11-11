package com.google.ssadm.rinftechtest.repo;


import java.io.File;
import java.io.IOException;

public interface FilesToBQLoader {
    public void fileToBQ(File file, File shortFile) throws IOException, InterruptedException;
}
