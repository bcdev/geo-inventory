package com.bc.inventory.search.compressed;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.GeoDb;
import com.bc.inventory.search.GeoDbEntry;
import com.bc.inventory.search.GeoDbUpdater;
import com.bc.inventory.search.GeoIndex;
import com.bc.inventory.search.QuerySolver;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.S2Utils;
import com.bc.inventory.utils.Search;
import com.google.common.collect.Iterators;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompressedGeoDb implements GeoDb {

    private static final int DEFAULT_MAX_LEVEL = 4;
    
    private final int maxLevel;
    private final boolean useIndex;

    private DbFile.Reader reader;
    private QuerySolver querySolver;

    private boolean readCompletely;
    private final List<DbFile.Entry> entries;
    private final List<S2Integer.Coverage> coverageList;
    private final Map<S2Integer.Coverage, Integer> coverageMap;
    private final Set<String> pathSet;
    private GeoIndex index;

    public CompressedGeoDb() {
        this(DEFAULT_MAX_LEVEL, true);        
    }
    
    public CompressedGeoDb(int maxLevel, boolean useIndex) {
        this.maxLevel = maxLevel;
        this.useIndex = useIndex;
        this.entries = new ArrayList<>();
        this.coverageList = new ArrayList<>();
        this.coverageMap = new HashMap<>();
        this.pathSet = new HashSet<>();
    }

    @Override
    public void open(ImageInputStream iis) throws IOException {
        reader = new DbFile.Reader(iis, useIndex);
        reader.readIndex();
        index = new Index();
        querySolver = new QuerySolver(index);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
    
    public int size() {
        if (index != null) {
            return index.size();
        } else {
            return entries.size();
        }
    }

    @Override
    public Iterator<GeoDbEntry> entries() throws IOException {
        if (!readCompletely) {
            readAllEntries();
        }
        return Iterators.transform(entries.iterator(),
                                   entry -> new GeoDbEntry(entry.startTime,
                                                           entry.endTime,
                                                           entry.path,
                                                           S2Utils.asPolygon(entry.polygonBytes)));
    }

    @Override
    public GeoDbUpdater getDbUpdater() {
        return new Updater();
    }

    @Override
    public List<String> query(Constrain constrain) {
        if (querySolver == null) {
            throw new IllegalStateException("CompressedGeoDb not opened for querying");
        }
        return querySolver.query(constrain);
    }

    private void readAllEntries() throws IOException {
        readCompletely = true;
        if (reader == null) {
            return; // new geoDB
        }

        for (int coverageIndex = 0; coverageIndex < reader.numBitmaps(); coverageIndex++) {
            S2Integer.Coverage coverage = new S2Integer.Coverage(reader.getBitmap(coverageIndex));
            coverageList.add(coverage);
            coverageMap.put(coverage, coverageIndex);
        }

        int[] startTimes = reader.getStartTimes();
        int[] endTimes = reader.getEndTimes();
        for (int productIndex = 0; productIndex < startTimes.length; productIndex++) {
            int startTime = startTimes[productIndex];
            int endTime = endTimes[productIndex];
            int coverageIndex = -1;
            if (useIndex) {
                coverageIndex = reader.getBitmapIndex(productIndex);
            }
            index.readEntry(productIndex);
            byte[] polygonBytes = reader.getCurrentPolygonBytes();
            String path = index.getCurrentPath();
            entries.add(new DbFile.Entry(startTime, endTime, path, polygonBytes, coverageIndex));
            pathSet.add(path);
        }
    }

    private class Updater implements GeoDbUpdater {
        
        @Override
        public void addEntry(GeoDbEntry entry) throws IOException {
            if (!readCompletely) {
                readAllEntries();
            }
            int coverageId = -1;
            if (useIndex) {
                S2CellUnion s2CellUnion = S2Integer.createCellUnion(entry.getPolygon(), maxLevel);
                int[] intIds = S2Integer.cellUnion2Ints(s2CellUnion);
                S2Integer.Coverage s2IntCoverage = new S2Integer.Coverage(intIds);
                coverageId = getUniqeCoverageId(s2IntCoverage);
            }
            byte[] polygonBytes = S2Utils.asBytes(entry.getPolygon());

            String path = entry.getPath();
            if (!pathSet.contains(path)) {
                DbFile.Entry dbEntry = new DbFile.Entry(entry.getStartTime(), entry.getEndTime(), path, polygonBytes, coverageId);
                entries.add(dbEntry);
                pathSet.add(path);
            }
        }

        private int getUniqeCoverageId(S2Integer.Coverage s2IntCoverage) {
            Integer index = coverageMap.get(s2IntCoverage);
            if (index == null) {
                coverageList.add(s2IntCoverage);
                index = coverageList.size() - 1;
                coverageMap.put(s2IntCoverage, index);
            }
            return index;
        }

        @Override
        public void write(OutputStream os) throws IOException {
            entries.sort(Comparator.comparingInt(r -> r.startTime));
            try (DbFile.Writer writer = new DbFile.Writer(os, useIndex)) {
                writer.write(entries, coverageList);
            }
        }
    }

    private class Index implements GeoIndex {

        private S2Point lastPoint;
        private int lastPointAsInt;
        private S2Polygon lastPolygon;
        private int[] lastPolygonAsCoverage;

        @Override
        public int size() {
            if (reader != null) {
                return reader.getStartTimes().length;
            } else {
                return 0;
            }
        }

        @Override
        public int getStartTime(int productIndex) {
            return reader.getStartTimes()[productIndex];
        }

        @Override
        public int getEndTime(int productIndex) {
            return reader.getEndTimes()[productIndex];
        }

        @Override
        public int getIndexForTime(int startTime) {
            return Search.indexedBinarySearch(reader.getStartTimes(), startTime);
        }

        @Override
        public boolean approximationContainsPoint(int productIndex, S2Point point) {
            if (useIndex) {
                if (point != lastPoint) {
                    S2CellId lastPointAsS2CellId = S2CellId.fromPoint(point);
                    lastPointAsInt = S2Integer.asInt(lastPointAsS2CellId);
                    lastPoint = point;
                }
                int bitmapIndex = reader.getBitmapIndex(productIndex);
                int[] coverage = reader.getBitmap(bitmapIndex);
                return S2Integer.containsCellId(coverage, lastPointAsInt);
            } else {
                return true;
            }
        }

        @Override
        public boolean approximationIntersectsPolygon(int productIndex, S2Polygon polygon) {
            if (useIndex) {
                if (polygon != lastPolygon) {
                    lastPolygonAsCoverage = S2Integer.createS2IntIds(polygon, maxLevel);
                    lastPolygon = polygon;
                }
                int bitmapIndex = reader.getBitmapIndex(productIndex);
                int[] coverage = reader.getBitmap(bitmapIndex);
                return S2Integer.intersectsCellUnionFast(lastPolygonAsCoverage, coverage);
            } else {
                return true;
            }
        }

        @Override
        public void readEntry(int productIndex) throws IOException {
            reader.readEntry(productIndex);
        }

        @Override
        public S2Polygon getCurrentPolygon() throws IOException {
            return reader.getCurrentPolygon();
        }

        @Override
        public String getCurrentPath() throws IOException {
            return reader.getCurrentPath();
        }
    }
}
