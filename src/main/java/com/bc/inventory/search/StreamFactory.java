package com.bc.inventory.search;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Creates stream for the different Inventory implementations.
 */
public interface StreamFactory {

    InputStream createInputStream(String name) throws IOException;

    OutputStream createOutputStream(String name) throws IOException;

    boolean exists(String dataFilename) throws IOException;
}
