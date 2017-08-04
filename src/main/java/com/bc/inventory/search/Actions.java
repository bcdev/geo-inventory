package com.bc.inventory.search;

import com.bc.inventory.search.compressed.CompressedGeoDb;
import com.bc.inventory.search.csv.CsvGeoDb;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class Actions {

    private final StreamFactory streamFactory;
    private final String indexFilename;
    private final int maxLevel;
    private final boolean useIndex;

    public Actions(StreamFactory streamFactory, String indexFilename, int maxLevel, boolean useIndex) {
        this.streamFactory = streamFactory;
        this.indexFilename = indexFilename;
        this.maxLevel = maxLevel;
        this.useIndex = useIndex;
    }

    public int updateIndex(String... filenames) throws IOException {
        CompressedGeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        if (streamFactory.exists(indexFilename)) {
            ImageInputStream iis = streamFactory.createImageInputStream(indexFilename);
            compressedGeoDb.open(iis);
        }
        for (String csvFile : filenames) {
            ImageInputStream iis = streamFactory.createImageInputStream(csvFile);
            CsvGeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(iis);
            Iterator<GeoDbEntry> entries = csvGeoDb.entries();
            while (entries.hasNext()) {
                compressedGeoDb.addEntry(entries.next());
            }
        }
        compressedGeoDb.close();
        OutputStream os = streamFactory.createOutputStream(indexFilename);
        return compressedGeoDb.write(os);
    }

    public QueryResult query(Constrain constrain) throws IOException {
        CompressedGeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        ImageInputStream iis = streamFactory.createImageInputStream(indexFilename);
        compressedGeoDb.open(iis);
        try {
            return compressedGeoDb.query(constrain);
        } finally {
            compressedGeoDb.close();        
        }
    }

    // dump

}
