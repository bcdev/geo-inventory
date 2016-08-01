package com.bc.inventory.search;

import com.bc.inventory.utils.DateUtils;
import com.bc.inventory.utils.SimpleRecord;
import com.google.common.geometry.S2Polygon;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.List;


/**
 * Search constrains
 */
public class Constrain {
    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");

    private final String name;
    private S2Polygon polygon;
    private long start = -1;
    private long end = -1;
    private List<SimpleRecord> insitu;
    private long delta = -1;
    private int numResults = Integer.MAX_VALUE;

    public Constrain(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public S2Polygon getPolygon() {
        return polygon;
    }

    public long getStartTime() {
        return start;
    }

    public long getEndTime() {
        return end;
    }

    public List<SimpleRecord> getInsitu() {
        return insitu;
    }

    public long getDelta() {
        return delta;
    }

    public int getNumResults() {
        return numResults;
    }

    public Constrain withPolygon(S2Polygon polygon) {
        this.polygon = polygon;
        return this;
    }

    public Constrain withStartTime(String start) {
        this.start = dateAsLong(start);
        return this;
    }

    public Constrain withEndTime(String end) {
        this.end = dateAsLong(end);
        return this;
    }

    public Constrain withInsitu(List<SimpleRecord> insitu) {
        this.insitu = insitu;
        return this;
    }

    public Constrain withDeltaTime(long delta) {
        this.delta = delta;
        return this;
    }

    public Constrain withNumResults(int numResults) {
        this.numResults = numResults;
        return this;
    }

    private static long dateAsLong(String dateString) {
        if (dateString != null && !dateString.isEmpty()) {
            try {
                return DATE_FORMAT.parse(dateString).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
                return -1;
            }
        } else {
            return -1;
        }
    }
}
