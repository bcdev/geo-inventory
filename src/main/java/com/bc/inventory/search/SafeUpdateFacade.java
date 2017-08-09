package com.bc.inventory.search;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SafeUpdateFacade implements Facade {
    
    private static final DateFormat ATTIC_DATE_FORMAT = TimeUtils.createDateFormat("yyyyMMdd_HHmmss_SSS");

    private final StreamFactory streamFactory;
    private final String dbDir;
    private final String indexFilenameA;
    private final String indexFilenameB;
    private final String indexFilenameNew;
    private final int maxLevel;
    private final boolean useIndex;

    public SafeUpdateFacade(StreamFactory streamFactory, String dbDir) {
        this(streamFactory, dbDir, 4, true);
    }

    public SafeUpdateFacade(StreamFactory streamFactory, String dbDir, int maxLevel, boolean useIndex) {
        this.streamFactory = streamFactory;
        this.dbDir = dbDir;
        this.indexFilenameA = dbDir + "/geo_index.a";
        this.indexFilenameB = dbDir + "/geo_index.b";
        this.indexFilenameNew = dbDir + "/geo_index.new";
        this.maxLevel = maxLevel;
        this.useIndex = useIndex;
    }

    @Override
    public int updateIndex(String... filenames) throws IOException {
        // list ".a" and ".b" sort by age
        GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        String[] indexFiles = streamFactory.listByAge(indexFilenameA, indexFilenameB);
        if (indexFiles.length > 0) {
            // read the newest
            ImageInputStream iis = streamFactory.createImageInputStream(indexFiles[0]);
            compressedGeoDb.open(iis);
        }
        GeoDbUpdater dbUpdater = compressedGeoDb.getDbUpdater();
        int addedProducts = SimpleFacade.updateFromCSV(dbUpdater, filenames, streamFactory);
        compressedGeoDb.close();

        if (streamFactory.exists(indexFilenameNew)) {
            System.err.println("'new' index does already exist. File will be overwritten: " + indexFilenameNew);
        }

        OutputStream os = streamFactory.createOutputStream(indexFilenameNew);
        dbUpdater.write(os);

        // remove older one from ".a" and ".b"
        String olderIndex = null;
        if (indexFiles.length == 0) {
            olderIndex = indexFilenameA;
        } else if (indexFiles.length == 1) {
            if (indexFiles[0].endsWith(".a")) {
                olderIndex = indexFilenameB;
            } else {
                olderIndex = indexFilenameA;
            }
        } else if (indexFiles.length == 2) {
            olderIndex = indexFiles[0];
        }
        // rename ".new" to older name
        streamFactory.rename(indexFilenameNew, olderIndex);

        // TODO concat csvFiles to attic/<TIMESTAMP>
        streamFactory.concat(filenames, dbDir+ "/attic/" + ATTIC_DATE_FORMAT.format(new Date(System.currentTimeMillis())) + ".csv");
        for (String filename : filenames) {
            streamFactory.delete(filename);
        }
        return addedProducts;
    }

    @Override
    public List<String> query(Constrain constrain) throws IOException {
        List<GeoDb> dbList = new ArrayList<>();
        // list ".a" and ".b" sort by age
        GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
        String[] indexFiles = streamFactory.listByAge(indexFilenameA, indexFilenameB);
        if (indexFiles.length > 0) {
            // read the newest
            ImageInputStream iis = streamFactory.createImageInputStream(indexFiles[0]);
            compressedGeoDb.open(iis);
            dbList.add(compressedGeoDb);
        }
        String[] csvFiles = streamFactory.listWithPrefix(dbDir, "CSV");

        for (String csvFile : csvFiles) {
            ImageInputStream iis = streamFactory.createImageInputStream(csvFile);
            CsvGeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(iis);
            dbList.add(csvGeoDb);
        }
        Set<String> resultSet = new HashSet<>();
        for (GeoDb geoDb : dbList) {
            resultSet.addAll(geoDb.query(constrain));
        }
        return new ArrayList<>(resultSet);
    }

    @Override
    public void dump(String csvFile) throws IOException {
        OutputStream os;
        if (csvFile == null) {
            os = System.out;
        } else {
            os = streamFactory.createOutputStream(csvFile);
        }
        try (Writer csvWriter = new BufferedWriter(new OutputStreamWriter(os))) {
            // list ".a" and ".b" sort by age
            String[] indexFiles = streamFactory.listByAge(indexFilenameA, indexFilenameB);
            if (indexFiles.length > 0) {
                // read the newest
                ImageInputStream iis = streamFactory.createImageInputStream(indexFiles[0]);
                GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
                compressedGeoDb.open(iis);
                SimpleFacade.dumpEntries(compressedGeoDb.entries(), csvWriter);
                compressedGeoDb.close();
            }
            String[] csvFiles = streamFactory.listWithPrefix(dbDir, "CSV");

            for (String csvFilePath : csvFiles) {
                ImageInputStream iis = streamFactory.createImageInputStream(csvFilePath);
                CsvGeoDb csvGeoDb = new CsvGeoDb();
                csvGeoDb.open(iis);
                SimpleFacade.dumpEntries(csvGeoDb.entries(), csvWriter);
            }
        }
    }
}
