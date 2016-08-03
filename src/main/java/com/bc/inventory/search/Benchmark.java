package com.bc.inventory.search;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.inventory.insitu.CsvRecordSource;
import com.bc.inventory.insitu.Record;
import com.bc.inventory.search.ng.NgInventory;
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
import java.util.List;

/**
 * A benchmark for comparing different geo inventory solutions.
 */
public class Benchmark {

    private static final long HOURS_IN_MILLIS = 1000 * 60 * 60; // Note: time in ms (NOT h)

    private static final String ACADIA_WKT = "polygon((-71.00 41.00, -52.00 41.00, -52.00 52.00, -71.00 52.00, -71.00 41.00))";
    private static final String NORTHSEA_WKT = "polygon((-19.94 40.00, -20.00 60.00, 0.0 60.00, 0.00 65.00, 13.06 65.00, 12.99 53.99, 0.00 49.22,  0.00 40.00,  -19.94 40.00))";

    private static List<Constrain> constrains;
    private static File baseDir;

    public static void main(String[] args) throws Exception {
        baseDir = new File(args[0]);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            System.out.printf("args[0] = '%s' is not a directory%n", args[0]);
            System.exit(1);
        }
        constrains = createConstrains();

//        measure("test");

//        measure("meris2005");
//        measure("modis2005");

        measure("meris");
        measure("modis");

    }

    private static void measure(String sensor) throws IOException {
        MeasurementTable mt = new MeasurementTable(sensor);
        String productListFilename = new File(baseDir, sensor + "_products_list.csv").getAbsolutePath();

//        testQueries("CSV", mt, new CsvInventory(productListFilename));
//        testQueries("CsvFast", mt, new CsvFastInventory(productListFilename));
        {
            StreamFactory streamFactory = new FileStreamFactory(new File(baseDir, sensor + "_l3"));
//            testIndexCreation("Ng3", mt, productListFilename, new NgInventory(streamFactory, false, 3));

            testQueries("NgI3", mt, new NgInventory(streamFactory, true, 3));
            testQueries("Ng3.1", mt, new NgInventory(streamFactory, false, 3));
            testQueries("Ng3.2", mt, new NgInventory(streamFactory, false, 3));
        }

        {
            StreamFactory streamFactory = new FileStreamFactory(new File(baseDir, sensor + "_l5"));
//            testIndexCreation("Ng5", mt, productListFilename, new NgInventory(streamFactory, false, 5));

            testQueries("NgI5", mt, new NgInventory(streamFactory, true, 5));
            testQueries("Ng5.1", mt, new NgInventory(streamFactory, false, 5));
            testQueries("Ng5.2", mt, new NgInventory(streamFactory, false, 5));
        }
        mt.printMeasurements();
    }

    private static void testIndexCreation(String label, MeasurementTable mt, String productListFilename, Inventory inventory) throws IOException {
        try (Measurement m = new Measurement("create index", label, mt)) {
            m.setNumProducts(inventory.createIndex(productListFilename));
        }
    }

    private static void testQueries(String label, MeasurementTable mt, Inventory inventory) throws IOException {
        try (Measurement m = new Measurement("load inventory", label, mt)) {
            m.setNumProducts(inventory.loadIndex());
        }
        for (Constrain constrain : constrains) {
            try (Measurement m = new Measurement(constrain.getQueryName(), label, mt)) {
                QueryResult queryResult = inventory.query(constrain);
                m.setNumProducts(queryResult.getPaths().size());
            }
        }
    }

    private static List<Constrain> createConstrains() throws Exception {
        S2WKTReader wktReader = new S2WKTReader();
        S2Polygon northseaPoly = (S2Polygon) wktReader.read(NORTHSEA_WKT);
        S2Polygon acadiaPoly = (S2Polygon) wktReader.read(ACADIA_WKT);
        List<SimpleRecord> latLonTime = readInsituRecords(new File(baseDir, "insitu.csv"));
        List<SimpleRecord> latLon = readInsituRecords(new File(baseDir, "extracts.csv"));

        Constrain c1 = new Constrain("northsea").withPolygon(northseaPoly);
        Constrain c2 = new Constrain("acadia, 1 year").withPolygon(acadiaPoly).withStartTime("2005-01-01").withEndTime("2006-01-01");
        Constrain c3 = new Constrain("northsea, 1 year").withPolygon(northseaPoly).withStartTime("2005-01-01").withEndTime("2006-01-01");
        Constrain c4 = new Constrain("northsea, 1 year, #100").withPolygon(northseaPoly).withStartTime("2005-01-01").withEndTime("2006-01-01").withNumResults(100);
        Constrain c5 = new Constrain("northsea, 1 week").withPolygon(northseaPoly).withStartTime("2005-06-01").withEndTime("2005-06-07");
        Constrain c6 = new Constrain("northsea, 1 day").withPolygon(northseaPoly).withStartTime("2005-06-01").withEndTime("2005-06-02");
        Constrain c7 = new Constrain("matchups 30k lat/lon/time").withInsitu(latLonTime).withDeltaTime(HOURS_IN_MILLIS * 3);
        Constrain c8 = new Constrain("extracts 3 lat/lon").withInsitu(latLon);
        Constrain c9 = new Constrain("extracts 3 lat/lon, 100#").withInsitu(latLon).withNumResults(100);
        Constrain c10 = new Constrain("extracts 3 lat/lon, 1 year").withInsitu(latLon).withStartTime("2005-01-01").withEndTime("2006-01-01");
        Constrain c11 = new Constrain("1 year").withStartTime("2005-01-01").withEndTime("2006-01-01");
        Constrain c12 = new Constrain("1 year, 100#").withStartTime("2005-01-01").withEndTime("2006-01-01").withNumResults(100);
        return Arrays.asList(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12);
    }

    private static List<SimpleRecord> readInsituRecords(File file) throws Exception {
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
