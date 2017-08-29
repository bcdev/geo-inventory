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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SafeUpdateInventory implements Inventory {

    private static final DateFormat ATTIC_DATE_FORMAT = TimeUtils.createDateFormat("yyyyMMdd_HHmmss_SSS");

    private final StreamFactory streamFactory;
    private final String dbDir;
    private final String indexFilenameA;
    private final String indexFilenameB;
    private final String indexFilenameNew;
    private final int maxLevel;
    private final boolean useIndex;
    private final String prefix;

    public SafeUpdateInventory(StreamFactory streamFactory, String dbDir) {
        this(streamFactory, dbDir, 4, true);
    }

    public SafeUpdateInventory(StreamFactory streamFactory, String dbDir, int maxLevel, boolean useIndex) {
        this.streamFactory = streamFactory;
        this.dbDir = dbDir;
        this.indexFilenameA = dbDir + "/geo_index.a";
        this.indexFilenameB = dbDir + "/geo_index.b";
        this.indexFilenameNew = dbDir + "/geo_index.new";
        this.maxLevel = maxLevel;
        this.useIndex = useIndex;
        this.prefix = "CSV";
    }

    @Override
    public int updateIndex(String... filenames) throws IOException {
        String[] indexFiles = streamFactory.listNewestFirst(indexFilenameA, indexFilenameB);
        GeoDb compressedDb = openCompressedDB(indexFiles).orElseGet(()-> new CompressedGeoDb(maxLevel, useIndex));
        GeoDbUpdater dBUpdater = compressedDb.getDbUpdater();
        if (filenames.length == 0) {
            filenames = listIncrementalFiles();
        }
        int addedProducts = SimpleInventory.updateFromCSV(dBUpdater, filenames, streamFactory);
        compressedDb.close();

        if (streamFactory.exists(indexFilenameNew)) {
            System.err.println("'new' index does already exist. File will be overwritten: " + indexFilenameNew);
        }

        try (OutputStream os = streamFactory.createOutputStream(indexFilenameNew)) {
            dBUpdater.write(os);
        }

        // remove older one from ".a" and ".b"
        String olderDbName = null;
        if (indexFiles.length == 0) {
            olderDbName = indexFilenameA;
        } else if (indexFiles.length == 1) {
            if (indexFiles[0].endsWith(".a")) {
                olderDbName = indexFilenameB;
            } else {
                olderDbName = indexFilenameA;
            }
        } else if (indexFiles.length == 2) {
            olderDbName = indexFiles[0];
        }
        // rename ".new" to older name
        streamFactory.rename(indexFilenameNew, olderDbName);

        streamFactory.concat(filenames, dbDir + "/attic/" + ATTIC_DATE_FORMAT.format(new Date(System.currentTimeMillis())) + ".csv");
        for (String filename : filenames) {
            streamFactory.delete(filename);
        }
        return addedProducts;
    }

    @Override
    public List<String> query(Constrain constrain) throws IOException {
        List<GeoDb> dbList = new ArrayList<>();

        openCompressedDB().ifPresent(dbList::add);
        Collections.addAll(dbList, openIncrementalUpdateDB());

        Set<String> resultSet = new HashSet<>();
        try {
            for (GeoDb geoDb : dbList) {
                resultSet.addAll(geoDb.query(constrain));
            }
        } finally {
            for (GeoDb geoDb : dbList) {
                geoDb.close();
            }
        }
        return new ArrayList<>(resultSet);
    }

    @Override
    public void dump(String outputCsvFile) throws IOException {
        OutputStream os;
        boolean closeOS = false;
        if (outputCsvFile == null) {
            os = System.out;
        } else {
            os = streamFactory.createOutputStream(outputCsvFile);
            closeOS = true;
        }
        try (Writer csvWriter = new BufferedWriter(new OutputStreamWriter(os))) {
            Optional<GeoDb> compressedDB = openCompressedDB();
            if (compressedDB.isPresent()) {
                GeoDb geoDb = compressedDB.get();
                SimpleInventory.dumpEntries(geoDb.entries(), csvWriter);
                geoDb.close();
            }
            
            for (GeoDb updateDB : openIncrementalUpdateDB()) {
                SimpleInventory.dumpEntries(updateDB.entries(), csvWriter);
                updateDB.close();
            }
        } finally {
            if (closeOS) {
                os.close();
            }
        }
    }

    private Optional<GeoDb> openCompressedDB() throws IOException {
        return openCompressedDB(streamFactory.listNewestFirst(indexFilenameA, indexFilenameB));
    }

    private Optional<GeoDb> openCompressedDB(String[] indexFiles) throws IOException {
        if (indexFiles.length > 0) {
            // read the newest DB
            ImageInputStream iis = streamFactory.createImageInputStream(indexFiles[0]);
            GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
            compressedGeoDb.open(iis);
            return Optional.of(compressedGeoDb);
        }
        return Optional.empty();
    }

    private GeoDb[] openIncrementalUpdateDB() throws IOException {
        String[] updateCsvFiles = listIncrementalFiles();
        GeoDb[] updateDBs = new GeoDb[updateCsvFiles.length];
        for (int i = 0; i < updateCsvFiles.length; i++) {
            String csvFile = updateCsvFiles[i];
            ImageInputStream iis = streamFactory.createImageInputStream(csvFile);
            GeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(iis);
            updateDBs[i] = csvGeoDb;
        }
        return updateDBs;
    }

    private String[] listIncrementalFiles() throws IOException {
        return streamFactory.listWithPrefix(dbDir, prefix);
    }
}
