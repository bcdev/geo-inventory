package com.bc.inventory.search.coverage;

import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Index;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.QuerySolver;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * An inventory based on a list of coverages.
 */
public class CoverageInventory implements Inventory {

    private static final long MINUTES_PER_MILLI = 60 * 1000;
    private static final DateFormat DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final int DEFFAULT_MAX_LEVEL = 3;
    private static final String INDEX_FILENAME = "index";
    private static final String DATA_FILENAME = "data";

    private final StreamFactory streamFactory;
    private final boolean indexOnly;
    private final int maxLevel;

    private int[] startTimes;
    private int[] endTimes;
    private int[] coverageIndices;
    private int[] dataOffsets;
    private int[][] coverages;
    private QuerySolver querySolver;

    public CoverageInventory(StreamFactory streamFactory) {
        this(streamFactory, false, DEFFAULT_MAX_LEVEL);
    }

    public CoverageInventory(StreamFactory streamFactory, boolean indexOnly, int maxLevel) {
        this.streamFactory = streamFactory;
        this.indexOnly = indexOnly;
        this.maxLevel = maxLevel;
    }

    @Override
    public int createIndex(String productListFilename) throws IOException {
        IndexCreator indexCreator = new IndexCreator(maxLevel);
        addRecordsToIndex(productListFilename, indexCreator);
        writeIndex(indexCreator);
        return indexCreator.size();
    }

    @Override
    public int updateIndex(String productListFilename) throws IOException {
        IndexCreator indexCreator = new IndexCreator(maxLevel);
        try (InputStream indexIS = streamFactory.createInputStream(INDEX_FILENAME);
             InputStream dataIS = streamFactory.createInputStream(DATA_FILENAME)) {
            indexCreator.loadExistingIndex(indexIS, dataIS);
        }
        addRecordsToIndex(productListFilename, indexCreator);
        writeIndex(indexCreator);
        return indexCreator.size();
    }

    private void writeIndex(IndexCreator indexCreator) throws IOException {
        try (OutputStream indexOS = streamFactory.createOutputStream(INDEX_FILENAME);
             OutputStream dataOS = streamFactory.createOutputStream(DATA_FILENAME)) {
            indexCreator.write(indexOS, dataOS);
        }
    }

    private void addRecordsToIndex(String productListFilename, IndexCreator indexCreator) throws IOException {
        try (InputStream inputStream = streamFactory.createInputStream(productListFilename)) {
            CsvRecordReader.CsvRecordIterator iterator = CsvRecordReader.getIterator(inputStream);
            while (iterator.hasNext()) {
                CsvRecord r = iterator.next();
                indexCreator.addToIndex(r.getPath(), r.getStartTime(), r.getEndTime(), r.getS2Polygon());
            }
        }
    }

    @Override
    public int loadIndex() throws IOException {
        try (IndexFile.Reader indexFile = new IndexFile.Reader(streamFactory.createInputStream(INDEX_FILENAME))) {
            indexFile.readRecords();
            startTimes = indexFile.getStartTimes();
            endTimes = indexFile.getEndTimes();
            coverageIndices = indexFile.getCoverageIndices();
            dataOffsets = indexFile.getDataOffsets();
            coverages = indexFile.readCoverages();
        }
        Index index = new CoverageIndex(startTimes, endTimes, coverageIndices, coverages, maxLevel, dataOffsets);
        querySolver = new QuerySolver(index, indexOnly);

        return startTimes.length;
    }
    
    @Override
    public QueryResult query(Constrain constrain) {
        return querySolver.query(constrain);
    }

    public void dumpDB(String csvFile) throws IOException {
        try (
                DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(DATA_FILENAME));
                Writer csvWriter = new BufferedWriter(new FileWriter(csvFile))
        ) {
            for (int i = 0; i < dataOffsets.length; i++) {
                reader.seekTo(dataOffsets[i]);
                String wkt = S2WKTWriter.write(reader.readPolygon());
                String path = reader.readPath();
                String startTime = DATE_FORMAT.format(new Date(startTimes[i] * MINUTES_PER_MILLI));
                String endTime = DATE_FORMAT.format(new Date(endTimes[i] * MINUTES_PER_MILLI));
                csvWriter.write(String.format("%s\t%s\t%s\t%s%n", path, startTime, endTime, wkt));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class CoverageIndex extends Index {

        private final int[] coverageIndices;
        private final int[][] coverages;
        private final int maxLevel;
        private final int[] dataOffsets;
        private DataFile.Reader reader;

        private S2Point lastPoint;
        private int lastPointAsInt;

        private S2Polygon lastPolygon;
        private int[] lastPolygonAsCoverage;

        CoverageIndex(int[] startTimes, int[] endTimes, int[] coverageIndices, int[][] coverages, int maxLevel, int[] dataOffsets) {
            super(startTimes, endTimes);
            this.coverageIndices = coverageIndices;
            this.coverages = coverages;
            this.maxLevel = maxLevel;
            this.dataOffsets = dataOffsets;
        }

        @Override
        public boolean containsPoint(int productIndex, S2Point point) {
            if (point != lastPoint) {
                S2CellId lastPointAsS2CellId = S2CellId.fromPoint(point);
                lastPointAsInt = S2Integer.asInt(lastPointAsS2CellId);
                lastPoint = point;
            }
            int[] coverage = coverages[coverageIndices[productIndex]];
            return S2Integer.containsCellId(coverage, lastPointAsInt);
        }

        @Override
        public boolean intersectsPolygon(int productIndex, S2Polygon polygon) {
            if (polygon != lastPolygon) {
                lastPolygonAsCoverage = S2Integer.createS2IntIds(polygon, maxLevel);
                lastPolygon = polygon;
            }
            int[] coverage = coverages[coverageIndices[productIndex]];
            return S2Integer.intersectsCellUnionFast(lastPolygonAsCoverage, coverage);
        }

        @Override
        public void readEntry(int productIndex) throws IOException {
            int dataOffset = dataOffsets[productIndex];
            if (reader == null) {
                reader = new DataFile.Reader(streamFactory.createInputStream(DATA_FILENAME));
            } else if (reader.currentPos() > dataOffset) {
                reader.close();
                reader = new DataFile.Reader(streamFactory.createInputStream(DATA_FILENAME));
            }
            reader.seekTo(dataOffset);
        }

        @Override
        public S2Polygon getCurrentPolygon() throws IOException {
            return reader.readPolygon();
        }

        @Override
        public String getCurrentPath() throws IOException {
            return reader.readPath();
        }
    }
    
    // for debugging
    private void printProducts(List<Integer> productIDs) {
        Collections.sort(productIDs, (o1, o2) -> Integer.compare(dataOffsets[o1], dataOffsets[o2]));
        try (DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(DATA_FILENAME))) {
            for (Integer productID : productIDs) {
                reader.seekTo(dataOffsets[productID]);
                String path = reader.readPath();
                String start = DATE_FORMAT.format(new Date(startTimes[productID] * MINUTES_PER_MILLI));
                String end = DATE_FORMAT.format(new Date(endTimes[productID] * MINUTES_PER_MILLI));
                System.out.printf("%s  %s  %s%n", start, end, path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
