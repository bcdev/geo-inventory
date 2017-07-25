package com.bc.inventory.search.bitmap;

import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Index;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.QuerySolver;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.TimeUtils;
import com.bc.inventory.utils.S2Integer;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import javax.imageio.stream.ImageInputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.text.DateFormat;
import java.util.*;

/**
 * An inventory based on a list of bitmaps.
 */
public class BitmapInventory implements Inventory {

    private static final long MINUTES_PER_MILLI = 60 * 1000;
    private static final DateFormat DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final int DEFFAULT_MAX_LEVEL = 3;
    private static final String INDEX_FILENAME = "geo_index";

    private final StreamFactory streamFactory;
    private final boolean indexOnly;
    private final int maxLevel;

    private int[] startTimes;
    private int[] endTimes;
    private DbFile.Reader reader;
    private QuerySolver querySolver;

    public BitmapInventory(StreamFactory streamFactory) {
        this(streamFactory, false, DEFFAULT_MAX_LEVEL);
    }

    public BitmapInventory(StreamFactory streamFactory, boolean indexOnly, int maxLevel) {
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
        try (ImageInputStream iis = streamFactory.createImageInputStream(INDEX_FILENAME)) {
            indexCreator.loadExistingIndex(iis);
        }
        addRecordsToIndex(productListFilename, indexCreator);
        writeIndex(indexCreator);
        return indexCreator.size();
    }

    private void writeIndex(IndexCreator indexCreator) throws IOException {
        try (OutputStream os = streamFactory.createOutputStream(INDEX_FILENAME)) {
            indexCreator.write(os);
        }
    }

    private void addRecordsToIndex(String productListFilename, IndexCreator indexCreator) throws IOException {
        try (InputStream inputStream = streamFactory.createInputStream(productListFilename)) {
            CsvRecordReader.CsvRecordIterator iterator = CsvRecordReader.getIterator(inputStream);
            int counter = 0;
            while (iterator.hasNext()) {
                CsvRecord r = iterator.next();
                indexCreator.addToIndex(r.getPath(), r.getStartTime(), r.getEndTime(), r.getS2Polygon());
                counter++;
                if (counter % 1000 == 0) {
                    System.out.println("added " + counter);
                }
            }
        }
    }

    @Override
    public int loadIndex() throws IOException {
        reader = new DbFile.Reader(streamFactory.createImageInputStream(INDEX_FILENAME));
        reader.readIndex();
        startTimes = reader.getStartTimes();
        endTimes = reader.getEndTimes();
        int[] bitmapIndices = reader.getBitmapIndices();
        ImmutableRoaringBitmap[] bitmaps = reader.getBitmaps();
        
        Index index = new BitmapIndex(startTimes, endTimes, bitmapIndices, bitmaps, maxLevel, reader);
        querySolver = new QuerySolver(index, indexOnly);
        return startTimes.length;
    }

    @Override
    public QueryResult query(Constrain constrain) {
        return querySolver.query(constrain);
    }

    @Override
    public int numEntries() {
        return startTimes != null ? startTimes.length : 0;
    }

    public boolean hasIndex() throws IOException {
        return streamFactory.exists(INDEX_FILENAME);
    }

    public void dumpDB(String csvFile) throws IOException {
        try (Writer csvWriter = new BufferedWriter(new FileWriter(csvFile))) {
            for (int i = 0; i < startTimes.length; i++) {
                reader.readEntry(i);
                String path = reader.getCurrentPath();
                S2Polygon polygon = reader.getCurrentPolygon();
                String wkt = S2WKTWriter.write(polygon);
                String startTime = DATE_FORMAT.format(new Date(startTimes[i] * MINUTES_PER_MILLI));
                String endTime = DATE_FORMAT.format(new Date(endTimes[i] * MINUTES_PER_MILLI));
                csvWriter.write(String.format("%s\t%s\t%s\t%s%n", path, startTime, endTime, wkt));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    static class BitmapIndex extends Index {
        
        private final int[] bitmapIndices;
        private final ImmutableRoaringBitmap[] bitmaps;
        private final int maxLevel;
        private final DbFile.Reader dbReader;
        
        private S2Point lastPoint;
        private int lastPointAsInt;
        
        private S2Polygon lastPolygon;
        private ImmutableRoaringBitmap lastPolygonAsBitma;

        BitmapIndex(int[] startTimes, int[] endTimes, int[] bitmapIndices, ImmutableRoaringBitmap[] bitmaps, int maxLevel, DbFile.Reader dbReader) {
            super(startTimes, endTimes);
            this.bitmapIndices = bitmapIndices;
            this.bitmaps = bitmaps;
            this.maxLevel = maxLevel;
            this.dbReader = dbReader;
        }

        @Override
        public boolean containsPoint(int productIndex, S2Point point) {
            if (point != lastPoint) {
                S2CellId lastPointAsS2CellId = S2CellId.fromPoint(point);
                lastPointAsInt = S2Integer.asIntAtLevel(lastPointAsS2CellId, maxLevel);
                lastPoint = point;
            }
            ImmutableRoaringBitmap roaringBitmap = bitmaps[bitmapIndices[productIndex]];
            return roaringBitmap.contains(lastPointAsInt);
        }

        @Override
        public boolean intersectsPolygon(int productIndex, S2Polygon polygon) {
            if (polygon != lastPolygon) {
                lastPolygonAsBitma = S2Integer.createCoverageBitmap(polygon, maxLevel);
                lastPolygon = polygon;
            }
            ImmutableRoaringBitmap roaringBitmap = bitmaps[bitmapIndices[productIndex]];
            return ImmutableRoaringBitmap.intersects(roaringBitmap, lastPolygonAsBitma);
        }

        @Override
        public void readEntry(int productIndex) throws IOException {
            dbReader.readEntry(productIndex);            
        }

        @Override
        public S2Polygon getCurrentPolygon() throws IOException {
            return dbReader.getCurrentPolygon();
        }

        @Override
        public String getCurrentPath() throws IOException {
            return dbReader.getCurrentPath();
        }
    }
}
