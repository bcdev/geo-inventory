package com.bc.inventory.search;

import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.search.compressed.CompressedGeoDb;
import com.bc.inventory.search.csv.CsvGeoDb;
import com.bc.inventory.utils.TimeUtils;

import javax.imageio.stream.ImageInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Iterator;
import java.util.List;

public class SimpleFacade implements Facade {

    private static final DateFormat DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    
    private final StreamFactory streamFactory;
    private final String indexFilename;
    private final int maxLevel;
    private final boolean useIndex;

    public SimpleFacade(StreamFactory streamFactory, String indexPath) {
        this(streamFactory, indexPath, 4, true);
    }

    public SimpleFacade(StreamFactory streamFactory, String indexPath, int maxLevel, boolean useIndex) {
        this.streamFactory = streamFactory;
        this.indexFilename = indexPath;
        this.maxLevel = maxLevel;
        this.useIndex = useIndex;
    }

    @Override
    public int updateIndex(String... filenames) throws IOException {
        GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        if (streamFactory.exists(indexFilename)) {
            ImageInputStream iis = streamFactory.createImageInputStream(indexFilename);
            compressedGeoDb.open(iis);
        }
        GeoDbUpdater dbUpdater = compressedGeoDb.getDbUpdater();
        for (String csvFile : filenames) {
            ImageInputStream iis = streamFactory.createImageInputStream(csvFile);
            CsvGeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(iis);
            Iterator<GeoDbEntry> entries = csvGeoDb.entries();
            while (entries.hasNext()) {
                dbUpdater.addEntry(entries.next());
            }
        }
        compressedGeoDb.close();
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

    @Override
    public void dump(String csvFile) throws IOException {
        CompressedGeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        ImageInputStream iis = streamFactory.createImageInputStream(indexFilename);
        compressedGeoDb.open(iis);
        Iterator<GeoDbEntry> entries = compressedGeoDb.entries();

        OutputStream os = streamFactory.createOutputStream(csvFile);
        try (Writer csvWriter = new BufferedWriter(new OutputStreamWriter(os))) {
            while (entries.hasNext()) {
                GeoDbEntry geoDbEntry = entries.next();
                String path = geoDbEntry.getPath();
                String startTime = DATE_FORMAT.format(TimeUtils.minuteTimeAsDate(geoDbEntry.getStartTime()));
                String endTime = DATE_FORMAT.format(TimeUtils.minuteTimeAsDate(geoDbEntry.getEndTime()));
                String wkt = S2WKTWriter.write(geoDbEntry.getPolygon());
                csvWriter.write(String.format("%s\t%s\t%s\t%s%n", path, startTime, endTime, wkt));
            }
        }
    }
}
