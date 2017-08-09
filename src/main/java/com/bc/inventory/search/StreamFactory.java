package com.bc.inventory.search;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Creates stream for the different Inventory implementations.
 */
public interface StreamFactory {

    InputStream createInputStream(String path) throws IOException;

    ImageInputStream createImageInputStream(String path) throws IOException;

    OutputStream createOutputStream(String path) throws IOException;

    boolean exists(String path) throws IOException;
}
