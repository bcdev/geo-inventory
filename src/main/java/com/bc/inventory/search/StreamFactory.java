package com.bc.inventory.search;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Creates stream for the different Inventory implementations.
 */
public interface StreamFactory {

    ImageInputStream createImageInputStream(String path) throws IOException;

    OutputStream createOutputStream(String path) throws IOException;

    boolean exists(String path) throws IOException;

    /**
     * newest Files last
     * 
     * @param filenames
     * @return
     * @throws IOException
     */
    String[] listByAge(String...filenames) throws IOException;

    void rename(String oldName, String newName) throws IOException;

    String[] listWithPrefix(String dir, String prefix) throws IOException;

    void concat(String[] sourceFilenames, String destFilename) throws IOException;

    void delete(String filename) throws IOException;
}
