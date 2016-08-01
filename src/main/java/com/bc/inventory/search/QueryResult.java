package com.bc.inventory.search;

import java.util.ArrayList;
import java.util.Collection;

/**
 * the result of a query.
 */
public class QueryResult {
    private final Collection<String> paths;
    private final int lastProductId;
    // has more, roughly how many

    public QueryResult(Collection<String> paths, int lastProductId) {
        this.paths = paths;
        this.lastProductId = lastProductId;
    }

    public QueryResult(Collection<String> paths) {
        this.paths = paths;
        this.lastProductId = -1;
    }

    public Collection<String> getPaths() {
        return paths;
    }

    public int getLastProductId() {
        return lastProductId;
    }
}
