package com.bc.inventory.search.csv;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.GeoDb;
import com.bc.inventory.search.GeoDbEntry;
import com.bc.inventory.search.GeoIndex;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.QuerySolver;
import com.bc.inventory.utils.Search;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.collect.Iterators;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import com.sun.imageio.plugins.common.InputStreamAdapter;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class CsvGeoDb implements GeoDb, GeoIndex {

    private List<CsvRecord> csvRecordList;
    private int[] startTimes;
    private int[] endTimes;
    private QuerySolver querySolver;
    private int currentProductIndex;

    @Override
    public void open(ImageInputStream iis) throws IOException {
        csvRecordList = CsvRecordReader.readAllRecords(new InputStreamAdapter(iis));
        iis.close();
        csvRecordList.sort(Comparator.comparingLong(CsvRecord::getStartTime));

        startTimes = new int[csvRecordList.size()];
        endTimes = new int[csvRecordList.size()];
        for (int i = 0; i < startTimes.length; i++) {
            CsvRecord csvRecord = csvRecordList.get(i);
            startTimes[i] = TimeUtils.startTimeInMin(csvRecord.getStartTime());
            endTimes[i] = TimeUtils.endTimeInMin(csvRecord.getEndTime());
        }
        querySolver = new QuerySolver(this);
    }

    @Override
    public void close() throws IOException {
        // nothing
    }

    @Override
    public Iterator<GeoDbEntry> entries() throws IOException {
        return Iterators.transform(csvRecordList.iterator(),
                                   csvRecord -> {
                                       int startTime = TimeUtils.startTimeInMin(csvRecord.getStartTime());
                                       int endTime = TimeUtils.endTimeInMin(csvRecord.getEndTime());
                                       String path = csvRecord.getPath();
                                       S2Polygon s2Polygon = csvRecord.getS2Polygon();
                                       return new GeoDbEntry(startTime, endTime, path, s2Polygon);
                                   });
    }

    @Override
    public void addEntry(GeoDbEntry entry) throws IOException {
        // CSV creates no index
    }

    @Override
    public int write(OutputStream os) throws IOException {
        // CSV creates no index
        return 0;
    }

    @Override
    public QueryResult query(Constrain constrain) {
        return querySolver.query(constrain);
    }

    @Override
    public int size() {
        return csvRecordList.size();
    }

    @Override
    public int getStartTime(int productIndex) {
        return startTimes[productIndex];
    }

    @Override
    public int getEndTime(int productIndex) {
        return endTimes[productIndex];
    }

    @Override
    public int getIndexForTime(int currentStartTime) {
        return Search.indexedBinarySearch(startTimes, currentStartTime);
    }

    @Override
    public boolean containsPoint(int productIndex, S2Point point) {
        return true;
    }

    @Override
    public boolean intersectsPolygon(int productIndex, S2Polygon polygon) {
        return true;
    }

    @Override
    public void readEntry(int productIndex) throws IOException {
        currentProductIndex = productIndex;
    }

    @Override
    public S2Polygon getCurrentPolygon() throws IOException {
        return csvRecordList.get(currentProductIndex).getS2Polygon();
    }

    @Override
    public String getCurrentPath() throws IOException {
        return csvRecordList.get(currentProductIndex).getPath();
    }
}
