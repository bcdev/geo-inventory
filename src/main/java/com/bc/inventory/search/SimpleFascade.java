package com.bc.inventory.search;

import com.bc.inventory.search.compressed.CompressedGeoDb;
import com.bc.inventory.search.csv.CsvGeoDb;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

public class SimpleFascade implements Fascade {

    private final StreamFactory streamFactory;
    private final String indexFilename;
    private final int maxLevel;
    private final boolean useIndex;

    public SimpleFascade(StreamFactory streamFactory, String indexFilename, int maxLevel, boolean useIndex) {
        this.streamFactory = streamFactory;
        this.indexFilename = indexFilename;
        this.maxLevel = maxLevel;
        this.useIndex = useIndex;
    }

    @Override
    public int updateIndex(String... filenames) throws IOException {
        GeoDb db = new CompressedGeoDb(maxLevel, useIndex);
        if (streamFactory.exists(indexFilename)) {
            ImageInputStream iis = streamFactory.createImageInputStream(indexFilename);
            db.open(iis);
        }
        GeoDbUpdater dbUpdater = db.getDbUpdater();
        for (String csvFile : filenames) {
            ImageInputStream iis = streamFactory.createImageInputStream(csvFile);
            CsvGeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(iis);
            Iterator<GeoDbEntry> entries = csvGeoDb.entries();
            while (entries.hasNext()) {
                dbUpdater.addEntry(entries.next());
            }
        }
        db.close();
        OutputStream os = streamFactory.createOutputStream(indexFilename);
        return dbUpdater.write(os);
    }

    @Override
    public List<String> query(Constrain constrain) throws IOException {
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
