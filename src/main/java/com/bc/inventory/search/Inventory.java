package com.bc.inventory.search;

import java.io.IOException;
import java.util.Collection;

/**
 * An inventory.
 */
public interface Inventory {

    int createIndex(String productListFilename) throws IOException;

    int updateIndex(String productListFilename) throws IOException;

    int loadIndex() throws IOException;

    QueryResult query(Constrain constrain);

    int numEntries();

}
