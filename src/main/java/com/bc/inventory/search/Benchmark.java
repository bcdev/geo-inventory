package com.bc.inventory.search;

import com.bc.inventory.insitu.InsituRecords;
import com.bc.inventory.utils.Measurement;
import com.bc.inventory.utils.MeasurementTable;
import com.bc.inventory.utils.SimpleRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A benchmark for comparing different geo inventory solutions.
 */
public class Benchmark {

    private static final long HOURS_IN_MILLIS = 1000 * 60 * 60; // Note: time in ms (NOT h)

    private static final String ACADIA_WKT = "polygon((-71.00 41.00, -52.00 41.00, -52.00 52.00, -71.00 52.00, -71.00 41.00))";
    private static final String NORTHSEA_WKT = "polygon((-19.94 40.00, -20.00 60.00, 0.0 60.00, 0.00 65.00, 13.06 65.00, 12.99 53.99, 0.00 49.22,  0.00 40.00,  -19.94 40.00))";
    private static final String VICTORIA_WKT = "POLYGON((31.04736328125 0.856901647439813,35.35400390625 0.856901647439813,35.35400390625 -3.2173020581871374,31.04736328125 -3.2173020581871374,31.04736328125 0.856901647439813))";

    private static List<Constrain> constrains;
    private static File baseDir;

    public static void main(String[] args) throws Exception {
        baseDir = new File(args[0]);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            System.out.printf("args[0] = '%s' is not a directory%n", args[0]);
            System.exit(1);
        }
        constrains = createConstrains();

        MeasurementTable mt = new MeasurementTable("Benchmark");
        measureGeoDB(mt, "meris", 4, true);
        measureGeoDB(mt, "meris", 4, false);

        measureGeoDB(mt, "modis", 4, true);
        measureGeoDB(mt, "modis", 4, false);

        measureGeoDB(mt, "viirs", 4, true);
        measureGeoDB(mt, "viirs", 4, false);

        measureGeoDB(mt, "seawifs", 4, true);
        measureGeoDB(mt, "seawifs", 4, false);
        
        measureGeoDB(mt, "S2_L1C", 4, true);
        measureGeoDB(mt, "S2_L1C", 4, false);
        
        mt.printMeasurements();
    }

    private static void measureGeoDB(MeasurementTable mt, String sensor, int maxLevel, boolean useIndex) throws IOException {
        String label = sensor + "_l" + maxLevel + "_" + Boolean.toString(useIndex);
        String productListFilename = baseDir.getPath() + "/" + sensor + "_products_list.csv";
        StreamFactory streamFactory = new FileStreamFactory();

        String indexFilename = baseDir.getPath() + "/GEO/data_" + sensor + "_level" + Integer.toString(maxLevel);
        if (useIndex) {
            indexFilename = baseDir.getPath() + "/GEO/index_" + sensor + "_level" + Integer.toString(maxLevel);
        }
        Inventory inventory = new SimpleInventory(streamFactory, indexFilename, maxLevel, useIndex);

        if (!streamFactory.exists(indexFilename)) {
            try (Measurement m = new Measurement("create/update DB", label, mt)) {
                m.setNumProducts(inventory.updateIndex(productListFilename));
            }
        }

        for (Constrain constrain : constrains) {
            try (Measurement m = new Measurement(constrain.getQueryName(), label, mt)) {
                List<String> queryResult = inventory.query(constrain);
                m.setNumProducts(queryResult.size());
            }
        }
    }

    private static List<Constrain> createConstrains() throws Exception {
        List<SimpleRecord> latLonTime = InsituRecords.read(new File(baseDir, "insitu.csv"), SimpleRecord.INSITU_DATE_FORMAT);
        List<SimpleRecord> latLon = InsituRecords.read(new File(baseDir, "extracts.csv"), SimpleRecord.INSITU_DATE_FORMAT);

        List<Constrain.Builder> cbs = new ArrayList<>();
//        cbs.add(new Constrain.Builder("northsea").withPolygon(NORTHSEA_WKT));
//        cbs.add(new Constrain.Builder("northsea2").withPolygon(NORTHSEA_WKT));
//        cbs.add(new Constrain.Builder("acadia, 1 year").withPolygon(ACADIA_WKT).addDateRang("2005-01-01", "2006-01-01"));
        cbs.add(new Constrain.Builder("northsea, 1 year").withPolygon(NORTHSEA_WKT).addDateRang("2005-01-01", "2006-01-01"));
//        cbs.add(new Constrain.Builder("northsea, 1 year, #100").withPolygon(NORTHSEA_WKT).addDateRang("2005-01-01", "2006-01-01").withMaxNumResults(100));
//        cbs.add(new Constrain.Builder("northsea, 1 week").withPolygon(NORTHSEA_WKT).addDateRang("2005-06-01", "2005-06-07"));
//        cbs.add(new Constrain.Builder("victoria, 1 week").withPolygon(VICTORIA_WKT).addDateRang("2016-06-01", "2016-06-07"));
//        cbs.add(new Constrain.Builder("northsea, 1 day").withPolygon(NORTHSEA_WKT).addDateRang("2005-06-01", "2005-06-01"));
//        cbs.add(new Constrain.Builder("matchups 30k lat/lon/time").withInsituRecords(latLonTime).withInsituTimeDelta(HOURS_IN_MILLIS * 3));
//        cbs.add(new Constrain.Builder("extracts 3 lat/lon").withInsituRecords(latLon));
//        cbs.add(new Constrain.Builder("extracts 3 lat/lon, 100#").withInsituRecords(latLon).withMaxNumResults(100));
//        cbs.add(new Constrain.Builder("extracts 3 lat/lon, 1 year").withInsituRecords(latLon).addDateRang("2005-01-01", "2006-01-01"));
//        cbs.add(new Constrain.Builder("1 year").addDateRang("2005-01-01", "2006-01-01"));
//        cbs.add(new Constrain.Builder("1 year, 100#").addDateRang("2005-01-01", "2006-01-01").withMaxNumResults(100));
//        cbs.add(new Constrain.Builder("1 day").addDateRang("2005-06-01", "2005-06-01"));
//        cbs.add(new Constrain.Builder("1 day (only start)").addDateRang("2005-06-01", "2005-06-01").useOnlyProductStartDate(true));

        List<Constrain> constrains = new ArrayList<>();
        for (Constrain.Builder cb : cbs) {
            constrains.add(cb.build());
        }
        return constrains;
    }
}
