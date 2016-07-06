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

    public Constrain(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public S2Polygon getPolygon() {
        return polygon;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public List<SimpleRecord> getInsitu() {
        return insitu;
    }

    public long getDelta() {
        return delta;
    }

    public Constrain widthPolygon(S2Polygon polygon) {
        this.polygon = polygon;
        return this;
    }

    public Constrain widthStart(String start) {
        this.start = dateAsLong(start);
        return this;
    }

    public Constrain witdthEnd(String end) {
        this.end = dateAsLong(end);
        return this;
    }

    public Constrain widthInsitu(List<SimpleRecord> insitu) {
        this.insitu = insitu;
        return this;
    }

    public Constrain widthDeltaTime(long delta) {
        this.delta = delta;
        return this;
    }

    public static long dateAsLong(String dateString) {
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
