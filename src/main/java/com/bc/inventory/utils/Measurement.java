package com.bc.inventory.utils;

/**
 * Created by marcoz on 04.07.16.
 */
public class Measurement implements AutoCloseable {

    private final MeasurementTable measurementTable;
    private final long startTime;

    final String testName;
    final String engine;

    long ms;
    int numProducts;

    public Measurement(String testName, String engine, MeasurementTable measurementTable) {
        this.testName = testName;
        this.engine = engine;
        this.measurementTable = measurementTable;
        this.startTime = System.currentTimeMillis();
//        System.out.println("performing: " + testName + " on " + engine);
    }

    public void setNumProducts(int numProducts) {
        this.numProducts = numProducts;
    }

    @Override
    public void close() {
        long endtime = System.currentTimeMillis();
        ms = (endtime - startTime);
        measurementTable.addMeasurement(this);
    }
}
