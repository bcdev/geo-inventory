package com.bc.inventory.search.csv;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
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
public class CsvInventory implements Inventory {

    private final String sensor;
    private final StreamFactory streamFactory;

    private List<CsvRecord> csvRecordList;

    public CsvInventory(String sensor, StreamFactory  streamFactory) {
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
            CsvRecordReader csvRecordReader = new CsvRecordReader(inputStream);
            csvRecordList = csvRecordReader.getCsvRecordList();
            return csvRecordList.size();
        }
    }

    @Override
    public Collection<String> query(Constrain constrain) {
        List<SimpleRecord> insitu = constrain.getInsitu();
        long start = constrain.getStart();
        long end = constrain.getEnd();
        S2Polygon polygon = constrain.getPolygon();

        if (insitu == null) {
            return testPolygon(csvRecordList, start, end, polygon);
        } else {
            Set<String> results = new HashSet<>();
            for (SimpleRecord insituRecord : insitu) {
                long delta = constrain.getDelta();
                if (delta != -1) {
                    start = insituRecord.getTime() - delta;
                    end = insituRecord.getTime() + delta;
                }
                results.addAll(testPoint(csvRecordList, start, end, insituRecord.getAsPoint()));
            }
            ArrayList<String> strings = new ArrayList<>(results);
            Collections.sort(strings);
            return strings;
        }
    }

    private Collection<String> testPolygon(List<CsvRecord> csvRecordList, long start, long end, S2Polygon polygon) {
        List<String> results = new ArrayList<>();
        for (CsvRecord csvRecord : csvRecordList) {
            if (start != -1 && csvRecord.getEndTime() < start) {
                continue;
            }
            if (end != -1 && csvRecord.getStartTime() > end) {
                continue;
            }
            if (polygon != null && csvRecord.getS2Polygon().intersects(polygon)) {
                results.add(csvRecord.getPath());
            }
        }
        return results;
    }

    private Collection<String> testPoint(List<CsvRecord> csvRecordList, long start, long end, S2Point point) {
        List<String> results = new ArrayList<>();
        for (CsvRecord csvRecord : csvRecordList) {
            if (start != -1 && csvRecord.getEndTime() < start) {
                continue;
            }
            if (end != -1 && csvRecord.getStartTime() > end) {
                continue;
            }
            if (csvRecord.getS2Polygon().contains(point)) {
                results.add(csvRecord.getPath());
            }
        }
        return results;
    }

}
