package com.bc.inventory.search;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Creates Stream based on Files
 */
public class FileStreamFactory implements StreamFactory {

    private final File baseDir;

    public FileStreamFactory(File baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public InputStream createInputStream(String name) throws IOException {
        return new FileInputStream(new File(baseDir, name));
    }

    @Override
    public OutputStream createOutputStream(String name) throws IOException {
        File file = new File(baseDir, name);
        File parentFile = file.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        return new FileOutputStream(file);
    }

    @Override
    public boolean exists(String name) {
        return new File(baseDir, name).exists();
    }
}
