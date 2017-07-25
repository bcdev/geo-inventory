package com.bc.inventory.search;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.utils.TimeUtils;
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

    private static final DateFormat DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd");
    private static final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000L;

    private final String queryName;
    private final S2Polygon polygon;
    private final long start;
    private final long end;
    private final boolean useOnlyProductStart;
    private final SimpleRecord[] insituRecords;
    private final long timeDelta;
    private final int maxNumResults;

    private Constrain(String queryName, S2Polygon polygon, long start, long end, boolean useOnlyProductStart, SimpleRecord[] insituRecords, long timeDelta, int maxNumResults) {
        this.queryName = queryName;
        this.polygon = polygon;
        this.start = start;
        this.end = end;
        this.useOnlyProductStart = useOnlyProductStart;
        this.insituRecords = insituRecords;
        this.timeDelta = timeDelta;
        this.maxNumResults = maxNumResults;
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

    public boolean useOnlyProductStart() {
        return useOnlyProductStart;
    }

    public SimpleRecord[] getInsituRecords() {
        return insituRecords;
    }

    public long getTimeDelta() {
        return timeDelta;
    }

    public int getMaxNumResults() {
        return maxNumResults;
    }

    @Override
    public String toString() {
        String wkt = (polygon != null) ? S2WKTWriter.write(polygon) : "null";
        return "Constrain{" +
                "queryName='" + queryName + '\'' +
                ", polygon=" + wkt +
                ", start=" + start +
                ", end=" + end +
                ", useOnlyProductStart=" + useOnlyProductStart +
                ", insituRecords=" + insituRecords.length +
                ", timeDelta=" + timeDelta +
                ", maxNumResults=" + maxNumResults +
                '}';
    }

    public static class Builder {

        private final String queryName;
        private S2Polygon s2Polygon = null;
        private long start = -1;
        private long end = -1;
        private SimpleRecord[] insituRecords = new SimpleRecord[0];
        private long timeDelta = -1;
        private int maxNumResults = Integer.MAX_VALUE;
        private boolean useOnlyProductStart;

        public Builder() {
            this("");
        }

        public Builder(String queryName) {
            this.queryName = queryName;
        }

        public Constrain.Builder polygon(S2Polygon polygon) {
            this.s2Polygon = polygon;
            return this;
        }

        public Constrain.Builder polygon(String polygonWKT) {
            S2WKTReader wktReader = new S2WKTReader();
            Object object = wktReader.read(polygonWKT);
            if (!(object instanceof S2Polygon)) {
                throw new IllegalArgumentException("Given polygonWKT is not a valid polygon.");
            }
            return polygon((S2Polygon) object);
        }

        public Constrain.Builder startDate(String start) {
            this.start = dateAsLong(start);
            return this;
        }

        public Constrain.Builder startDate(Date start) {
            if (start != null) {
                this.start = start.getTime();
            }
            return this;
        }

        public Builder endDate(String end) {
            this.end = dateAsLong(end);
            if (this.end != -1) {
                // end date is inclusive
                this.end += DAY_IN_MILLIS;
            }
            return this;
        }

        public Constrain.Builder endDate(Date end) {
            if (end != null) {
                this.end = end.getTime();
                // end date is inclusive
                this.end += DAY_IN_MILLIS;
            }
            return this;
        }

        public Constrain.Builder useOnlyProductStartDate(boolean onlyProductStart) {
            this.useOnlyProductStart = onlyProductStart;
            return this;
        }

        public Constrain.Builder insitu(List<SimpleRecord> insituRecords) {
            this.insituRecords = insituRecords.toArray(new SimpleRecord[0]);
            return this;
        }

        public Constrain.Builder timeDelta(long timeDelta) {
            this.timeDelta = timeDelta;
            return this;
        }

        public Constrain.Builder maxNumResults(int maxNumResults) {
            this.maxNumResults = maxNumResults;
            return this;
        }

        public Constrain build() {
            return new Constrain(queryName, s2Polygon, start, end, useOnlyProductStart, insituRecords, timeDelta, maxNumResults);
        }
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
