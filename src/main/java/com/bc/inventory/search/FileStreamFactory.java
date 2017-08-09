package com.bc.inventory.search;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
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


    @Override
    public InputStream createInputStream(String path) throws IOException {
        return new FileInputStream(new File(path));
    }

    @Override
    public ImageInputStream createImageInputStream(String path) throws IOException {
        return new FileImageInputStream(new File(path));
    }

    @Override
    public OutputStream createOutputStream(String path) throws IOException {
        File file = new File(path);
        File parentFile = file.getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        return new FileOutputStream(file);
    }

    @Override
    public boolean exists(String path) {
        return new File(path).exists();
    }
}
