package com.bc.inventory.search.csv;

import com.google.common.geometry.S2Polygon;

/**
 * Created by marcoz on 04.07.16.
 */
public class CsvRecord {

    private final String path;
    private final long startTime;
    private final long endTime;
    private final S2Polygon s2Polygon;

    CsvRecord(String path, long startTime, long endTime, S2Polygon s2Polygon) {
        this.path = path;
        this.startTime = startTime;
        this.endTime = endTime;
        this.s2Polygon = s2Polygon;
    }

    public String getPath() {
        return path;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public S2Polygon getS2Polygon() {
        return s2Polygon;
    }
}
