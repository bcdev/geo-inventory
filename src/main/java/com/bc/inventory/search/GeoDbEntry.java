package com.bc.inventory.search;

import com.google.common.geometry.S2Polygon;

/**
 * An entry in the GeoDb
 */
public class GeoDbEntry {

    private final int startTime;
    private final int endTime;
    private final String path;
    private final S2Polygon polygon;

    public GeoDbEntry(int startTime, int endTime, String path, S2Polygon polygon) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.path = path;
        this.polygon = polygon;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public String getPath() {
        return path;
    }

    public S2Polygon getPolygon() {
        return polygon;
    }
}
