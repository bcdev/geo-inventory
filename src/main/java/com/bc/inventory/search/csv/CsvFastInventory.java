package com.bc.inventory.search.csv;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.utils.SimpleRecord;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A inventory using just a ASCII CSV file as to read the information from.
 */
public class CsvFastInventory implements Inventory {

    private final File productListFile;
    private List<CsvRecord> csvRecordList;

    public CsvFastInventory(File productListFile) {
        this.productListFile = productListFile;
    }

    @Override
    public int createIndex(String productListFilename) throws IOException {
        // not required, no index in use
        return 0;
    }

    @Override
    public int updateIndex(String productListFilename) throws IOException {
        // not required, no index in use
        return 0;
    }

    @Override
    public int loadIndex() throws IOException {
        try (InputStream inputStream = new FileInputStream(productListFile)) {
            csvRecordList = CsvRecordReader.readAllRecords(inputStream);
            Collections.sort(csvRecordList, (o1, o2) -> Long.compare(o1.getStartTime(), o2.getStartTime()));
            return csvRecordList.size();
        }
    }

    @Override
    public QueryResult query(Constrain constrain) {
        SimpleRecord[] insituRecords = constrain.getInsituRecords();
        long start = constrain.getStartTime();
        long end = constrain.getEndTime();
        S2Polygon polygon = constrain.getPolygon();

        if (insituRecords.length == 0) {
            return new QueryResult(test(start, end, null, polygon));
        } else {
            Set<String> results = new HashSet<>();
            for (SimpleRecord insituRecord : insituRecords) {
                long timeDelta = constrain.getTimeDelta();
                if (timeDelta != -1) {
                    start = insituRecord.getTime() - timeDelta;
                    end = insituRecord.getTime() + timeDelta;
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

        int productIndex;
        if (startTime == -1) {
            productIndex = 0;
        } else {
            productIndex = getIndexForTime(startTime);
            if (productIndex == -1) {
                return results;
            }
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
        int high = csvRecordList.size() - 1;
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
