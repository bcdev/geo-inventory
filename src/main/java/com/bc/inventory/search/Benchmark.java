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
        Facade facade = new SimpleFacade(streamFactory, indexFilename, maxLevel, useIndex);

        if (!streamFactory.exists(indexFilename)) {
            try (Measurement m = new Measurement("create/update DB", label, mt)) {
                m.setNumProducts(facade.updateIndex(productListFilename));
            }
        }

        for (Constrain constrain : constrains) {
            try (Measurement m = new Measurement(constrain.getQueryName(), label, mt)) {
                List<String> queryResult = facade.query(constrain);
                m.setNumProducts(queryResult.size());
            }
        }
    }

    private static List<Constrain> createConstrains() throws Exception {
        List<SimpleRecord> latLonTime = InsituRecords.read(new File(baseDir, "insitu.csv"), SimpleRecord.INSITU_DATE_FORMAT);
        List<SimpleRecord> latLon = InsituRecords.read(new File(baseDir, "extracts.csv"), SimpleRecord.INSITU_DATE_FORMAT);

        List<Constrain.Builder> cbs = new ArrayList<>();
//        cbs.add(new Constrain.Builder("northsea").polygon(NORTHSEA_WKT));
//        cbs.add(new Constrain.Builder("northsea2").polygon(NORTHSEA_WKT));
//        cbs.add(new Constrain.Builder("acadia, 1 year").polygon(ACADIA_WKT).startDate("2005-01-01").endDate("2006-01-01"));
        cbs.add(new Constrain.Builder("northsea, 1 year").polygon(NORTHSEA_WKT).startDate("2005-01-01").endDate("2006-01-01"));
//        cbs.add(new Constrain.Builder("northsea, 1 year, #100").polygon(NORTHSEA_WKT).startDate("2005-01-01").endDate("2006-01-01").maxNumResults(100));
//        cbs.add(new Constrain.Builder("northsea, 1 week").polygon(NORTHSEA_WKT).startDate("2005-06-01").endDate("2005-06-07"));
//        cbs.add(new Constrain.Builder("victoria, 1 week").polygon(VICTORIA_WKT).startDate("2016-06-01").endDate("2016-06-07"));
//        cbs.add(new Constrain.Builder("northsea, 1 day").polygon(NORTHSEA_WKT).startDate("2005-06-01").endDate("2005-06-01"));
//        cbs.add(new Constrain.Builder("matchups 30k lat/lon/time").insitu(latLonTime).timeDelta(HOURS_IN_MILLIS * 3));
//        cbs.add(new Constrain.Builder("extracts 3 lat/lon").insitu(latLon));
//        cbs.add(new Constrain.Builder("extracts 3 lat/lon, 100#").insitu(latLon).maxNumResults(100));
//        cbs.add(new Constrain.Builder("extracts 3 lat/lon, 1 year").insitu(latLon).startDate("2005-01-01").endDate("2006-01-01"));
//        cbs.add(new Constrain.Builder("1 year").startDate("2005-01-01").endDate("2006-01-01"));
//        cbs.add(new Constrain.Builder("1 year, 100#").startDate("2005-01-01").endDate("2006-01-01").maxNumResults(100));
//        cbs.add(new Constrain.Builder("1 day").startDate("2005-06-01").endDate("2005-06-01"));
//        cbs.add(new Constrain.Builder("1 day (only start)").startDate("2005-06-01").endDate("2005-06-01").useOnlyProductStartDate(true));

        List<Constrain> constrains = new ArrayList<>();
        for (Constrain.Builder cb : cbs) {
            constrains.add(cb.build());
        }
        return constrains;
    }
}
