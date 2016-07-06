package com.bc.inventory.search;

import java.io.IOException;
import java.util.Collection;

/**
 * An inventory.
 */
public interface Inventory {

    int createIndex() throws IOException;

    int loadIndex() throws IOException;

    Collection<String> query(Constrain constrain);

}
