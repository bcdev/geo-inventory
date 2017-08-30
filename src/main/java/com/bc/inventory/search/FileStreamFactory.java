package com.bc.inventory.search;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Creates Stream based on Files
 */
public class FileStreamFactory implements StreamFactory {

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

    @Override
    public String[] listNewestFirst(String... filenames) throws IOException {
        List<File> existingFiles = new ArrayList<>();
        for (String filename : filenames) {
            File file = new File(filename);
            if (file.exists()) {
                existingFiles.add(file);
            }
        }
        existingFiles.sort(Comparator.comparingLong(File::lastModified));
        Collections.reverse(existingFiles);
        String[] result = new String[existingFiles.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = existingFiles.get(i).getPath();
        }
        return result;
    }

    @Override
    public void rename(String oldName, String newName) throws IOException {
        boolean success = new File(oldName).renameTo(new File(newName));
        if (!success) {
            throw new IOException(String.format("Failed to rename '%s' to '%s'", oldName, newName));
        }
    }

    @Override
    public String[] listWithPrefix(String dir, String prefix) throws IOException {
        String[] list = new File(dir).list((dir1, name) -> name.startsWith(prefix));
        if (list == null) {
            return new String[0];
        }
        String[] result = new String[list.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new File(dir, list[i]).getPath();
        }
        return result;
    }

    @Override
    public void concat(String[] sourceFilenames, String destFilename) throws IOException {
        try (OutputStream os = createOutputStream(destFilename)) {
            for (String sourceFilename : sourceFilenames) {
                Files.copy(new File(sourceFilename).toPath(), os);
            }
        }
    }

    @Override
    public void delete(String filename) throws IOException {
        boolean success = new File(filename).delete();
        if (!success) {
            throw new IOException(String.format("Failed to delete '%s'", filename));
        }
    }
}
