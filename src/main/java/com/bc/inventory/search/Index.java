package com.bc.inventory.search;

import com.bc.inventory.utils.Search;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;

public abstract class Index {
    private final int[] startTimes;
    private final int[] endTimes;

    public Index(int[] startTimes, int[] endTimes) {
        this.startTimes = startTimes;
        this.endTimes = endTimes;
    }
    
    public int size() {
        return startTimes.length;
    }

    public int getStartTime(int productIndex) {
        return startTimes[productIndex];
    }

    public int getEndTime(int productIndex) {
        return endTimes[productIndex];
    }
    
    public int getIndexForTime(int currentStartTime) {
        return Search.indexedBinarySearch(startTimes, currentStartTime);
    }
    
    public abstract boolean containsPoint(int productIndex, S2Point point);
    
    public abstract boolean intersectsPolygon(int productIndex, S2Polygon polygon);
    
    public abstract void readEntry(int productIndex) throws IOException;
    
    public abstract S2Polygon getCurrentPolygon() throws IOException;
    
    public abstract String getCurrentPath() throws IOException;
}
