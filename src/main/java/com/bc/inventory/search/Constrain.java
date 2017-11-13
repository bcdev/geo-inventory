package com.bc.inventory.search;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.utils.SimpleRecord;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.geometry.S2Polygon;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
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
    private final List<DateRange> dateRanges;
    private final boolean useOnlyProductStart;
    private final SimpleRecord[] insituRecords;
    private final long timeDelta;
    private final int maxNumResults;

    private Constrain(String queryName, S2Polygon polygon, List<DateRange> dateRanges, boolean useOnlyProductStart, SimpleRecord[] insituRecords, long timeDelta, int maxNumResults) {
        this.queryName = queryName;
        this.polygon = polygon;
        this.dateRanges = dateRanges;
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

    public List<DateRange> getDateRanges() {
        return dateRanges;
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
                ", #dateRanges=" + dateRanges.size() +
                ", dateRanges=" + dateRanges.toString() +
                ", useOnlyProductStart=" + useOnlyProductStart +
                ", insituRecords=" + insituRecords.length +
                ", timeDelta=" + timeDelta +
                ", maxNumResults=" + maxNumResults +
                '}';
    }

    public static class Builder {

        private final String queryName;
        private final List<DateRange> dateRanges;
        private final List<SimpleRecord> insituRecords;
        private S2Polygon s2Polygon = null;
        private long timeDelta = -1;
        private int maxNumResults = Integer.MAX_VALUE;
        private boolean useOnlyProductStart;

        public Builder() {
            this("");
        }

        public Builder(String queryName) {
            this.queryName = queryName;
            this.dateRanges = new ArrayList<>();
            this.insituRecords = new ArrayList<>();
        }

        public Constrain.Builder withPolygon(S2Polygon polygon) {
            this.s2Polygon = polygon;
            return this;
        }

        public Constrain.Builder withPolygon(String polygonWKT) {
            S2WKTReader wktReader = new S2WKTReader();
            Object object = wktReader.read(polygonWKT);
            if (!(object instanceof S2Polygon)) {
                throw new IllegalArgumentException("Given polygonWKT is not a valid polygon.");
            }
            return withPolygon((S2Polygon) object);
        }
        
        public Constrain.Builder addDateRang(String start, String end) {
            addDateRang(dateStringAsDate(start), dateStringAsDate(end));
            return this;
        }

        public Constrain.Builder addDateRang(Date start, Date end) {
            long startMillis = -1;
            long endMillis = -1;
            if (start != null) {
                startMillis = start.getTime();
            }
            if (end != null) {
                // end date is inclusive
                endMillis = end.getTime();
                endMillis += DAY_IN_MILLIS;
            }
            dateRanges.add(new DateRange(startMillis, endMillis));
            return this;
        }

        public Constrain.Builder useOnlyProductStartDate(boolean onlyProductStart) {
            this.useOnlyProductStart = onlyProductStart;
            return this;
        }

        public Constrain.Builder withInsituRecords(List<SimpleRecord> insituRecords) {
            this.insituRecords.addAll(insituRecords);
            return this;
        }

        public Constrain.Builder withInsituTimeDelta(long timeDelta) {
            this.timeDelta = timeDelta;
            return this;
        }

        public Constrain.Builder withMaxNumResults(int maxNumResults) {
            this.maxNumResults = maxNumResults;
            return this;
        }

        public Constrain build() {
            if (dateRanges.isEmpty()) {
                dateRanges.add(new DateRange(-1, -1));
            }
            return new Constrain(queryName, 
                                 s2Polygon, 
                                 dateRanges, 
                                 useOnlyProductStart, 
                                 insituRecords.toArray(new SimpleRecord[0]), 
                                 timeDelta, 
                                 maxNumResults);
        }
    }

    private static Date dateStringAsDate(String dateString) {
        if (dateString != null && !dateString.isEmpty() && !dateString.equalsIgnoreCase("null")) {
            try {
                return DATE_FORMAT.parse(dateString);
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }
    
    private static String longAsDateString(long date) {
        if (date <= -1L) {
            return "null";
        } else {
            return DATE_FORMAT.format(new Date(date));
        }
    }
    
    public static class DateRange {
        private final long start;
        private final long end;

        public DateRange(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        @Override
        public String toString() {
            return longAsDateString(start) +", " + longAsDateString(end - DAY_IN_MILLIS);
        }
    }
}
