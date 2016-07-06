package com.bc.inventory.search;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.inventory.insitu.CsvRecordSource;
import com.bc.inventory.insitu.Record;
import com.bc.inventory.search.coverage.CoverageInventory;
import com.bc.inventory.search.csv.CsvInventory;
import com.bc.inventory.utils.Measurement;
import com.bc.inventory.utils.MeasurementTable;
import com.bc.inventory.utils.SimpleRecord;
import com.google.common.geometry.S2Polygon;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A benchmark for comparing different geo inventory solutions.
 */
public class Benchmark {

    static final long HOURS_IN_MILLIS = 1000 * 60 * 60; // Note: time in ms (NOT h)

    static final String ACADIA_WKT = "polygon((-71.00 41.00, -52.00 41.00, -52.00 52.00, -71.00 52.00, -71.00 41.00))";
    static final String NORTHSEA_WKT = "polygon((-19.94 40.00, -20.00 60.00, 0.0 60.00, 0.00 65.00, 13.06 65.00, 12.99 53.99, 0.00 49.22,  0.00 40.00,  -19.94 40.00))";

    private static List<Constrain> constrains;
    private static File baseDir;

    public static void main(String[] args) throws Exception {
        baseDir = new File(args[0]);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            System.out.printf("args[0] = '%s' is not a directory%n", args[0]);
            System.exit(1);
        }
        StreamFactory streamFactory = new FileStreamFactory(baseDir);
        constrains = createConstrains();

        MeasurementTable meris = new MeasurementTable("MERIS");
        MeasurementTable modis = new MeasurementTable("MODIS");

        // CSV
//        testQueries("CSV", mtTest, new CsvInventory("test", streamFactory));
        testQueries("Csv_1", meris, new CsvInventory("meris", streamFactory));
        testQueries("Csv_2", meris, new CsvInventory("meris", streamFactory));
        testQueries("Csv_3", meris, new CsvInventory("meris", streamFactory));

        testQueries("Csv_1", modis, new CsvInventory("modis", streamFactory));
        testQueries("Csv_2", modis, new CsvInventory("modis", streamFactory));
        testQueries("Csv_3", modis, new CsvInventory("modis", streamFactory));

        // Coverage
//        testIndexCreation("TEST", "Coverage", mtTest, new CoverageInventory("test", streamFactory));
//        testQueries("TEST", "Coverage", mtTest, new CoverageInventory("test", streamFactory));


//        testIndexCreation("MERIS", "Coverage", mtTest, new CoverageInventory("meris", streamFactory));

        testQueries("Coverage_1", meris, new CoverageInventory("meris", streamFactory));
        testQueries("Coverage_2", meris, new CoverageInventory("meris", streamFactory));
        testQueries("Coverage_3", meris, new CoverageInventory("meris", streamFactory));

        testQueries("Coverage_1", modis, new CoverageInventory("modis", streamFactory));
        testQueries("Coverage_2", modis, new CoverageInventory("modis", streamFactory));
        testQueries("Coverage_3", modis, new CoverageInventory("modis", streamFactory));

//        testIndexCreation("MODIS", new CoverageInventory("modis", streamFactory));
//        testQueries("MODIS", new CoverageInventory("modis", streamFactory));

        meris.printMeasurements();
        modis.printMeasurements();
    }

    private static void testIndexCreation(String engine, MeasurementTable mt, Inventory inventory) throws IOException {
        try (Measurement m = new Measurement("create index", engine, mt)) {
            m.setNumProducts(inventory.createIndex());
        }
    }

    private static void testQueries(String engine, MeasurementTable mt, Inventory inventory) throws IOException {
        try (Measurement m = new Measurement("load inventory", engine, mt)) {
            m.setNumProducts(inventory.loadIndex());
        }
        for (Constrain constrain : constrains) {
            try (Measurement m = new Measurement(constrain.getName(), engine, mt)) {
                Collection<String> results = inventory.query(constrain);
                m.setNumProducts(results.size());
            }
        }
    }

    static List<Constrain> createConstrains() throws Exception {
        S2WKTReader wktReader = new S2WKTReader();
        S2Polygon northseaPoly = (S2Polygon) wktReader.read(NORTHSEA_WKT);
        List<SimpleRecord> latLonTime = readInsituRecords(new File(baseDir, "insitu.csv"));
        List<SimpleRecord> latLon = readInsituRecords(new File(baseDir, "extracts.csv"));

        Constrain c1 = new Constrain("northsea").widthPolygon(northseaPoly);
        Constrain c2 = new Constrain("northsea, 1 year").widthPolygon(northseaPoly).widthStart("2005-01-01").witdthEnd("2006-01-01");
        Constrain c3 = new Constrain("northsea, 1 week").widthPolygon(northseaPoly).widthStart("2005-06-01").witdthEnd("2005-06-07");
        Constrain c4 = new Constrain("northsea, 1 day").widthPolygon(northseaPoly).widthStart("2005-06-01").witdthEnd("2005-06-02");
        Constrain c5 = new Constrain("matchups 30k lat/lon/time").widthInsitu(latLonTime).widthDeltaTime(HOURS_IN_MILLIS * 3);
        Constrain c6 = new Constrain("extracts 3 lat/lon").widthInsitu(latLon);
        Constrain c7 = new Constrain("extracts 3 lat/lon, 1 year").widthInsitu(latLon).widthStart("2005-01-01").witdthEnd("2006-01-01");
        return Arrays.asList(c1, c2, c3, c4, c5, c6, c7);
    }

    static List<SimpleRecord> readInsituRecords(File file) throws Exception {
        try (Reader reader = new LineNumberReader(new FileReader(file), 100 * 1024)) {
            CsvRecordSource recordSource = new CsvRecordSource(reader, SimpleRecord.INSITU_DATE_FORMAT);
            boolean hasTime = recordSource.getHeader().hasTime();
            List<SimpleRecord> records = new ArrayList<>();
            for (Record record : recordSource.getRecords()) {
                if (hasTime) {
                    records.add(new SimpleRecord(record.getTime().getTime(), record.getLocation()));
                } else {
                    records.add(new SimpleRecord(-1, record.getLocation()));
                }
            }
            return records;
        }
    }
}
