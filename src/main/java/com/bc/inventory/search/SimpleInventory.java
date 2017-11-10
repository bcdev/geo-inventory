package com.bc.inventory.search;

import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.search.compressed.CompressedGeoDb;
import com.bc.inventory.search.csv.CsvGeoDb;
import com.bc.inventory.utils.TimeUtils;

import javax.imageio.stream.ImageInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Iterator;
import java.util.List;

public class SimpleInventory implements Inventory {

    private static final DateFormat DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private final StreamFactory streamFactory;
    private final String indexFilename;
    private final int maxLevel;
    private final boolean useIndex;

    public SimpleInventory(StreamFactory streamFactory, String indexPath) {
        this(streamFactory, indexPath, 4, true);
    }

    public SimpleInventory(StreamFactory streamFactory, String indexPath, int maxLevel, boolean useIndex) {
        this.streamFactory = streamFactory;
        this.indexFilename = indexPath;
        this.maxLevel = maxLevel;
        this.useIndex = useIndex;
    }

    @Override
    public int updateIndex(String... filenames) throws IOException {
        GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        if (streamFactory.exists(indexFilename)) {
            compressedGeoDb.open(streamFactory.createInputStream(indexFilename));
        }

        GeoDbUpdater dbUpdater = compressedGeoDb.getDbUpdater();
        int addedProducts = updateFromCSV(dbUpdater, filenames, streamFactory);
        compressedGeoDb.close();

        OutputStream os = streamFactory.createOutputStream(indexFilename);
        dbUpdater.write(os);
        return addedProducts;
    }

    @Override
    public List<String> query(Constrain constrain) throws IOException {
        CompressedGeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        if (!streamFactory.exists(indexFilename)) {
            throw new IllegalArgumentException("geo index does not exits:" + indexFilename);
        }
        compressedGeoDb.open(streamFactory.createInputStream(indexFilename));
        try {
            return compressedGeoDb.query(constrain);
        } finally {
            compressedGeoDb.close();
        }
    }

    @Override
    public void dump(String csvFile) throws IOException {
        CompressedGeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        if (!streamFactory.exists(indexFilename)) {
            throw new IllegalArgumentException("geo index does not exits:" + indexFilename);
        }
        compressedGeoDb.open(streamFactory.createInputStream(indexFilename));

        OutputStream os;
        if (csvFile == null) {
            os = System.out;
        } else {
            os = streamFactory.createOutputStream(csvFile);
        }
        try (Writer csvWriter = new BufferedWriter(new OutputStreamWriter(os))) {
            dumpEntries(compressedGeoDb.entries(), csvWriter);
        }
        compressedGeoDb.close();
    }

    static void dumpEntries(Iterator<GeoDbEntry> entries, Writer csvWriter) throws IOException {
        while (entries.hasNext()) {
            GeoDbEntry geoDbEntry = entries.next();
            String path = geoDbEntry.getPath();
            String startTime = formatTime(geoDbEntry.getStartTime());
            String endTime = formatTime(geoDbEntry.getEndTime());
            String wkt = S2WKTWriter.write(geoDbEntry.getPolygon());
            csvWriter.write(String.format("%s\t%s\t%s\t%s%n", path, startTime, endTime, wkt));
        }
    }

    private static String formatTime(int time) {
        if (time < 0) {
            return "null";
        }
        return DATE_FORMAT.format(TimeUtils.minuteTimeAsDate(time));
    }

    static int updateFromCSV(GeoDbUpdater dbUpdater, String[] filenames, StreamFactory streamFactory) throws IOException {
        int counter = 0;
        for (String csvFile : filenames) {
            CsvGeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(streamFactory.createInputStream(csvFile));
            Iterator<GeoDbEntry> entries = csvGeoDb.entries();
            while (entries.hasNext()) {
                dbUpdater.addEntry(entries.next());
                counter++;
            }
        }
        return counter;
    }
}
