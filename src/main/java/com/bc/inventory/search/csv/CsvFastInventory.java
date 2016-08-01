package com.bc.inventory.search.csv;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.utils.SimpleRecord;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A inventory using just a ASCII CSV file as to read the information from.
 */
public class CsvFastInventory implements Inventory {

    private final String sensor;
    private final StreamFactory streamFactory;

    private List<CsvRecord> csvRecordList;

    public CsvFastInventory(String sensor, StreamFactory streamFactory) {
        this.sensor = sensor;
        this.streamFactory = streamFactory;
    }

    @Override
    public int createIndex() throws IOException {
        return 0;
    }

    @Override
    public int loadIndex() throws IOException {
        try (InputStream inputStream = streamFactory.createInputStream(sensor + "_products_list.csv")) {
            csvRecordList = CsvRecordReader.readAllRecords(inputStream);
            Collections.sort(csvRecordList, (o1, o2) -> Long.compare(o1.getStartTime(), o2.getStartTime()));
            return csvRecordList.size();
        }
    }

    @Override
    public QueryResult query(Constrain constrain) {
        List<SimpleRecord> insitu = constrain.getInsitu();
        long start = constrain.getStartTime();
        long end = constrain.getEndTime();
        S2Polygon polygon = constrain.getPolygon();

        if (insitu == null) {
            return new QueryResult(test(start, end, null, polygon));
        } else {
            Set<String> results = new HashSet<>();
            for (SimpleRecord insituRecord : insitu) {
                long delta = constrain.getDelta();
                if (delta != -1) {
                    start = insituRecord.getTime() - delta;
                    end = insituRecord.getTime() + delta;
                }
                results.addAll(test(start, end, insituRecord.getAsPoint(), null));
            }
            ArrayList<String> strings = new ArrayList<>(results);
            Collections.sort(strings);
            return new QueryResult(strings);
        }
    }

    private List<String> test(long startTime, long endTime, S2Point point, S2Polygon polygon) {
        List<String> results = new ArrayList<>();
        int productIndex = getIndexForTime(startTime);
        if (productIndex == -1) {
            return Collections.EMPTY_LIST;
        }

        boolean finishedWithInsitu = false;
        while (!finishedWithInsitu) {
            CsvRecord record = null;
            if (productIndex < csvRecordList.size()) {
                record = csvRecordList.get(productIndex);
            }

            if (record == null) {
                finishedWithInsitu = true;
            } else if (endTime != -1 && record.getStartTime() > endTime) {
                finishedWithInsitu = true;
            } else if (startTime != -1 && record.getEndTime() < startTime) {
                //test next product;
            } else {
                // time matches, now test geo
                if (point != null) {
                    if (record.getS2Polygon().contains(point)) {
                        results.add(Integer.toString(productIndex));
                    }
                } else if (polygon != null) {
                    if (record.getS2Polygon().intersects(polygon)) {
                        results.add(Integer.toString(productIndex));
                    }
                }
            }
            productIndex++;
        }
        return results;
    }

    private int getIndexForTime(long startTime) {
        int low = 0;
        int high = csvRecordList.size() -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            final long t1 = csvRecordList.get(mid).getStartTime();
            if (t1 < startTime) {
                low = mid + 1;
            } else if (t1 == startTime) {
                return mid; // key found
            } else {
                high = mid - 1;
            }
        }
        return low == 0 ? low : low - 1;  // key not found
    }
}
