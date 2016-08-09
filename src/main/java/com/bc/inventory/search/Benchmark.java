package com.bc.inventory.search;

import com.bc.inventory.insitu.CsvRecordSource;
import com.bc.inventory.insitu.Record;
import com.bc.inventory.search.coverage.CoverageInventory;
import com.bc.inventory.utils.Measurement;
import com.bc.inventory.utils.MeasurementTable;
import com.bc.inventory.utils.SimpleRecord;

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

//            testQueries("NgI3", mt, new CoverageInventory(streamFactory, true, 3));
            testQueries("Ng3.1", mt, new CoverageInventory(streamFactory, false, 3));
            testQueries("Ng3.2", mt, new CoverageInventory(streamFactory, false, 3));
        }

        {
            StreamFactory streamFactory = new FileStreamFactory(new File(baseDir, sensor + "_l5"));
//            testIndexCreation("Ng5", mt, productListFilename, new NgInventory(streamFactory, false, 5));

//            testQueries("NgI5", mt, new CoverageInventory(streamFactory, true, 5));
            testQueries("Ng5.1", mt, new CoverageInventory(streamFactory, false, 5));
            testQueries("Ng5.2", mt, new CoverageInventory(streamFactory, false, 5));
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
        List<SimpleRecord> latLonTime = readInsituRecords(new File(baseDir, "insitu.csv"));
        List<SimpleRecord> latLon = readInsituRecords(new File(baseDir, "extracts.csv"));

        Constrain c1 = new Constrain.Builder("northsea").polygon(NORTHSEA_WKT).build();
        Constrain c2 = new Constrain.Builder("acadia, 1 year").polygon(ACADIA_WKT).startDate("2005-01-01").endDate("2006-01-01").build();
        Constrain c3 = new Constrain.Builder("northsea, 1 year").polygon(NORTHSEA_WKT).startDate("2005-01-01").endDate("2006-01-01").build();
        Constrain c4 = new Constrain.Builder("northsea, 1 year, #100").polygon(NORTHSEA_WKT).startDate("2005-01-01").endDate("2006-01-01").maxNumResults(100).build();
        Constrain c5 = new Constrain.Builder("northsea, 1 week").polygon(NORTHSEA_WKT).startDate("2005-06-01").endDate("2005-06-07").build();
        Constrain c6 = new Constrain.Builder("northsea, 1 day").polygon(NORTHSEA_WKT).startDate("2005-06-01").endDate("2005-06-02").build();
        Constrain c7 = new Constrain.Builder("matchups 30k lat/lon/time").insitu(latLonTime).timeDelta(HOURS_IN_MILLIS * 3).build();
        Constrain c8 = new Constrain.Builder("extracts 3 lat/lon").insitu(latLon).build();
        Constrain c9 = new Constrain.Builder("extracts 3 lat/lon, 100#").insitu(latLon).maxNumResults(100).build();
        Constrain c10 = new Constrain.Builder("extracts 3 lat/lon, 1 year").insitu(latLon).startDate("2005-01-01").endDate("2006-01-01").build();
        Constrain c11 = new Constrain.Builder("1 year").startDate("2005-01-01").endDate("2006-01-01").build();
        Constrain c12 = new Constrain.Builder("1 year, 100#").startDate("2005-01-01").endDate("2006-01-01").maxNumResults(100).build();
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
