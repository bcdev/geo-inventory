package com.bc.inventory.search;

import java.io.IOException;
import java.io.OutputStream;

public interface GeoDbUpdater {
    
    void addEntry(GeoDbEntry entry) throws IOException;

    void write(OutputStream os) throws IOException;
    
}
