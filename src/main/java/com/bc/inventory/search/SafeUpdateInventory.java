package com.bc.inventory.search;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SafeUpdateInventory implements Inventory {

    private static final DateFormat ATTIC_DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd_HH-mm-ss_SSS");

    private final StreamFactory streamFactory;
    private final String dbDir;
    private final String indexFilenameA;
    private final String indexFilenameB;
    private final String indexFilenameNew;
    private final int maxLevel;
    private final boolean useIndex;
    private boolean verbose;
    private boolean failOnMissingDB;
    private String updatePrefix;
    private String atticPrefix;
    private String atticSuffix;

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
        this.verbose = true;
        this.failOnMissingDB = false;
        this.updatePrefix = "CSV";
        this.atticPrefix = "scan.";
        this.atticSuffix = ".csv";
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setFailOnMissingDB(boolean failOnMissingDB) {
        this.failOnMissingDB = failOnMissingDB;
    }

    public void setUpdatePrefix(String updatePrefix) {
        this.updatePrefix = updatePrefix;
    }

    public void setAtticPrefix(String atticPrefix) {
        this.atticPrefix = atticPrefix;
    }

    public void setAtticSuffix(String atticSuffix) {
        this.atticSuffix = atticSuffix;
    }

    @Override
    public int updateIndex(String... filenames) throws IOException {
        long t1 = System.currentTimeMillis();
        if (filenames.length == 0) {
            filenames = listIncrementalFiles();
        }
        if (filenames.length == 0) {
            printVerbose("updateIndex: no update files available");
        } else {
            printVerbose("updateIndex: update with " + Arrays.toString(filenames));
        }
        String[] indexFiles = streamFactory.listNewestFirst(indexFilenameA, indexFilenameB);
        GeoDb compressedDb = openCompressedDB(indexFiles).orElseGet(() -> new CompressedGeoDb(maxLevel, useIndex));
        GeoDbUpdater dBUpdater = compressedDb.getDbUpdater();
        
        int addedProducts = SimpleInventory.updateFromCSV(dBUpdater, filenames, streamFactory);
        if (addedProducts == 0) {
            printVerbose("updateIndex: update file contains no entries, skip writing");
            compressedDb.close();
            moveScansToAttic(filenames);
            return addedProducts;
        }
        printVerbose(String.format("updateIndex: added %s products, new size %s", addedProducts, compressedDb.size()));
        compressedDb.close();

        if (streamFactory.exists(indexFilenameNew)) {
            System.err.println("'new' index does already exist. File will be overwritten: " + indexFilenameNew);
        }

        try (OutputStream os = streamFactory.createOutputStream(indexFilenameNew)) {
            printVerbose("updateIndex: writing compressed DB to " + indexFilenameNew);
            dBUpdater.write(os);
        }

        // remove older one from ".a" and ".b"
        String oldDbName = null;
        if (indexFiles.length == 0) {
            oldDbName = indexFilenameA;
        } else if (indexFiles.length == 1) {
            if (indexFiles[0].endsWith(".a")) {
                oldDbName = indexFilenameB;
            } else {
                oldDbName = indexFilenameA;
            }
        } else if (indexFiles.length == 2) {
            oldDbName = indexFiles[1];
        }
        printVerbose(String.format("updateIndex: renaming  (%s) -> (%s)", indexFilenameNew, oldDbName));
        // rename ".new" to older name
        streamFactory.rename(indexFilenameNew, oldDbName);

        moveScansToAttic(filenames);
        long t2 = System.currentTimeMillis();
        printVerbose(String.format("updateIndex: took %,d ms", t2 - t1));
        return addedProducts;
    }

    private void moveScansToAttic(String[] filenames) throws IOException {
        String atticName = atticPrefix + ATTIC_DATE_FORMAT.format(new Date()) + atticSuffix;
        String atticPath = "/attic/" + atticName;
        streamFactory.concat(filenames, dbDir + atticPath);
        printVerbose("updateIndex: creating archive " + dbDir + atticPath);
        for (String filename : filenames) {
            printVerbose("updateIndex: deleting " + filename);
            streamFactory.delete(filename);
        }
    }

    @Override
    public List<String> query(Constrain constrain) throws IOException {
        if (constrain == null) {
            throw new NullPointerException("constrain");
        }
        long t1 = System.currentTimeMillis();
        if (verbose) {
            printVerbose("query: constrain " + constrain);
        }
        List<GeoDb> dbList = new ArrayList<>();
        openCompressedDB().ifPresent(dbList::add);
        Collections.addAll(dbList, openUpdateDBs());
        
        if (dbList.isEmpty() && failOnMissingDB) {
            throw new IOException(String.format("Inventory does not exist: '%s'", dbDir));
        }

        Set<String> resultSet = new HashSet<>();
        try {
            for (int dbIndex = 0; dbIndex < dbList.size(); dbIndex++) {
                GeoDb geoDb = dbList.get(dbIndex);
                String dbClassName = geoDb.getClass().getSimpleName();
                List<String> result = geoDb.query(constrain);
                int numResults = result.size();
                if (numResults > 0) {
                    printVerbose(String.format("query: (db %s : %s) #results=%d", dbIndex, dbClassName, numResults));
                    resultSet.addAll(result);
                }
            }
        } finally {
            for (GeoDb geoDb : dbList) {
                geoDb.close();
            }
        }
        long t2 = System.currentTimeMillis();
        printVerbose(String.format("query: took %,d ms", t2 - t1));
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

            for (GeoDb updateDB : openUpdateDBs()) {
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
        printVerbose("openCompressedDB from: " + Arrays.toString(indexFiles));
        if (indexFiles.length > 0) {
            // read the newest DB
            InputStream is = streamFactory.createInputStream(indexFiles[0]);
            GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
            compressedGeoDb.open(is);
            printVerbose("openCompressedDB size: " + compressedGeoDb.size());
            return Optional.of(compressedGeoDb);
        }
        return Optional.empty();
    }

    private GeoDb[] openUpdateDBs() throws IOException {
        String[] updateFiles = listIncrementalFiles();
        GeoDb[] updateDBs = new GeoDb[updateFiles.length];
        for (int i = 0; i < updateFiles.length; i++) {
            String csvFile = updateFiles[i];
            InputStream is = streamFactory.createInputStream(csvFile);
            GeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(is);
            printVerbose(String.format("openUpdateDBs (%s) from: %s (size: %s)", i, csvFile, csvGeoDb.size()));
            updateDBs[i] = csvGeoDb;
        }
        return updateDBs;
    }

    private String[] listIncrementalFiles() throws IOException {
        return streamFactory.listWithPrefix(dbDir, updatePrefix);
    }

    private void printVerbose(String message) {
        if (verbose) {
            System.out.println("geoDB " + message);
        }
    }
}
