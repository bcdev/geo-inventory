package com.bc.inventory.search;

import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;

public interface GeoIndex {

    int size();

    int getStartTime(int productIndex);

    int getEndTime(int productIndex);

    int getIndexForTime(int currentStartTime);

    boolean containsPoint(int productIndex, S2Point point);

    boolean intersectsPolygon(int productIndex, S2Polygon polygon);

    void readEntry(int productIndex) throws IOException;

    S2Polygon getCurrentPolygon() throws IOException;

    String getCurrentPath() throws IOException;

}
