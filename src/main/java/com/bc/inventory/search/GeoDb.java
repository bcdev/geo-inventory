package com.bc.inventory.search;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

/**
 * A "database" that stores information about EO products to enable
 * spatial-temporal searching
 */
public interface GeoDb {
    
    void open(ImageInputStream iis) throws IOException;
    
    void close() throws IOException;

    Iterator<GeoDbEntry> entries() throws IOException;
    
    GeoDbUpdater getDbUpdater();

    List<String> query(Constrain constrain);
}
