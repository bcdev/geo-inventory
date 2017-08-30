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
import java.util.Arrays;
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
    private String prefix;
    private boolean verbose;

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
        this.verbose = true;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public int updateIndex(String... filenames) throws IOException {
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
            olderDbName = indexFiles[1];
        }
        printVerbose(String.format("updateIndex: renaming  (%s) -> (%s)", indexFilenameNew, olderDbName));
        // rename ".new" to older name
        streamFactory.rename(indexFilenameNew, olderDbName);

        String atticFilename = "/attic/" + ATTIC_DATE_FORMAT.format(new Date(System.currentTimeMillis())) + ".csv";
        streamFactory.concat(filenames, dbDir + atticFilename);
        printVerbose("updateIndex: creating archive " + atticFilename);
        for (String filename : filenames) {
            printVerbose("updateIndex: deleting " + filename);
            streamFactory.delete(filename);
        }
        return addedProducts;
    }

    @Override
    public List<String> query(Constrain... constrains) throws IOException {
        if (constrains.length == 0) {
            return Collections.EMPTY_LIST;
        }
        if (verbose) {
            if (constrains.length == 1) {
                printVerbose("query: constrain " + constrains[0]);
            } else {
                for (int i = 0; i < constrains.length; i++) {
                    printVerbose(String.format("query: constrain[%s] %s", i, constrains[i]));
                }
            }
        }
        List<GeoDb> dbList = new ArrayList<>();
        openCompressedDB().ifPresent(dbList::add);
        Collections.addAll(dbList, openUpdateDBs());

        Set<String> resultSet = new HashSet<>();
        try {
            for (int dbIndex = 0; dbIndex < dbList.size(); dbIndex++) {
                GeoDb geoDb = dbList.get(dbIndex);
                for (int constrainIndex = 0; constrainIndex < constrains.length; constrainIndex++) {
                    Constrain constrain = constrains[constrainIndex];
                    List<String> result = geoDb.query(constrain);
                    printVerbose(String.format("query: (constrain %s)(db %s : %s) results %d", constrainIndex, dbIndex, geoDb.getClass().getSimpleName(), result.size()));
                    resultSet.addAll(result);
                }
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
            ImageInputStream iis = streamFactory.createImageInputStream(indexFiles[0]);
            GeoDb compressedGeoDb = new CompressedGeoDb(maxLevel, useIndex);
            compressedGeoDb.open(iis);
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
            printVerbose(String.format("openUpdateDBs (%s) from: %s", i, csvFile));
            ImageInputStream iis = streamFactory.createImageInputStream(csvFile);
            GeoDb csvGeoDb = new CsvGeoDb();
            csvGeoDb.open(iis);
            printVerbose(String.format("openUpdateDBs (%s) size: %s", i, csvGeoDb.size()));
            updateDBs[i] = csvGeoDb;
        }
        return updateDBs;
    }

    private String[] listIncrementalFiles() throws IOException {
        return streamFactory.listWithPrefix(dbDir, prefix);
    }

    private void printVerbose(String message) {
        if (verbose) {
            System.out.println("geoDB " + message);
        }
    }
}
