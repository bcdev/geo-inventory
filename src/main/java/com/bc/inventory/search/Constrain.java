package com.bc.inventory.search;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.inventory.utils.DateUtils;
import com.bc.inventory.utils.SimpleRecord;
import com.google.common.geometry.S2Polygon;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;


/**
 * Search constrains
 */
public class Constrain {
    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");
    private static final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000L;

    private final String queryName;
    private S2Polygon polygon;
    private long start = -1;
    private long end = -1;
    private List<SimpleRecord> insitu;
    private long delta = -1;
    private int numResults = Integer.MAX_VALUE;

    public Constrain() {
        this("");
    }

    public Constrain(String queryName) {
        this.queryName = queryName;
    }

    public String getQueryName() {
        return queryName;
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

    public Constrain withPolygon(String polygonWKT) {
        S2WKTReader wktReader = new S2WKTReader();
        Object object = wktReader.read(polygonWKT);
        if (!(object instanceof S2Polygon)) {
            throw new IllegalArgumentException("Given polygonWKT is not a valid polygon.");
        }
        this.polygon = (S2Polygon) object;
        return this;
    }

    public Constrain withStartDate(String start) {
        this.start = dateAsLong(start);
        return this;
    }

    public Constrain withEndDate(String end) {
        this.end = dateAsLong(end);
        if (this.end != -1) {
            // end date is inclusive
            this.end += DAY_IN_MILLIS;
        }
        return this;
    }

    public Constrain withStartDate(Date start) {
        if (start != null) {
            this.start = start.getTime();
        }
        return this;
    }

    public Constrain withEndDate(Date end) {
        if (end != null) {
            this.end = end.getTime();
            // end date is inclusive
            this.end += DAY_IN_MILLIS;
        }
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
