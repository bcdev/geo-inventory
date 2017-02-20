package com.bc.inventory.search.bitmap;

import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.DateUtils;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.Search;
import com.bc.inventory.utils.SimpleRecord;
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
    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final int DEFFAULT_MAX_LEVEL = 3;
    private static final String INDEX_FILENAME = "geo_index";

    private final StreamFactory streamFactory;
    private final boolean indexOnly;
    private final int maxLevel;

    private int[] startTimes;
    private int[] endTimes;
    private int[] bitmapIndices;
    private ImmutableRoaringBitmap[] bitmaps;
    private DbFile.Reader reader;

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
        bitmapIndices = reader.getBitmapIndices();
        bitmaps = reader.getBitmaps();
        return startTimes.length;
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

    @Override
    public QueryResult query(Constrain constrain) {
        SimpleRecord[] insituRecords = constrain.getInsituRecords();
        int start = IndexCreator.startTimeInMin(constrain.getStartTime());   // can be -1
        int end = IndexCreator.endTimeInMin(constrain.getEndTime());         // can be -1
        int maxNumResults = constrain.getMaxNumResults();

        if (insituRecords.length == 0) {
            S2Polygon polygon = constrain.getPolygon();
            boolean useOnlyProductStart = constrain.useOnlyProductStart();
            List<Integer> productIDs = testOnIndex(start, end, useOnlyProductStart, null, polygon);
            if (indexOnly) {
                return new QueryResult(testPolygonOnData(productIDs, null, maxNumResults));
            } else {
                return new QueryResult(testPolygonOnData(productIDs, polygon, maxNumResults));
            }
        } else {
            Map<Integer, List<S2Point>> candidatesMap = new HashMap<>();
            for (SimpleRecord insituRecord : insituRecords) {
                long delta = constrain.getTimeDelta();
                boolean useOnlyProductStart = constrain.useOnlyProductStart();
                long insituRecordTime = insituRecord.getTime();
                int insituStart = start;
                int insituEnd = end;
                if (delta != -1 && insituRecordTime != -1) {
                    insituStart = IndexCreator.startTimeInMin(insituRecordTime - delta);
                    insituEnd = IndexCreator.endTimeInMin(insituRecordTime + delta);
                    if (end != -1 && end < insituStart) {
                        continue;
                    }
                    if (start != -1 && start > insituEnd) {
                        continue;
                    }
                    useOnlyProductStart = false; // for time-matchups always precise time checks
                }
                S2Point s2Point = insituRecord.getAsPoint();
                List<Integer> productIDs = testOnIndex(insituStart, insituEnd, useOnlyProductStart, s2Point, null);
                if (!productIDs.isEmpty()) {
                    for (Integer match : productIDs) {
                        List<S2Point> candidateProducts = candidatesMap.get(match);
                        if (candidateProducts == null) {
                            candidateProducts = new ArrayList<>();
                            candidatesMap.put(match, candidateProducts);
                        }
                        candidateProducts.add(s2Point);
                    }
                }
            }
            List<String> paths;
            if (indexOnly) {
                paths = new ArrayList<>(candidatesMap.size());
                for (Integer productID : candidatesMap.keySet()) {
                    paths.add("index_only:" + productID);
                }
            } else {
                paths = testPointsOnData(candidatesMap, maxNumResults);
            }
            return new QueryResult(paths);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private List<Integer> testOnIndex(int startTime, int endTime, boolean useOnlyProductStart, S2Point point, S2Polygon polygon) {
        S2CellId s2CellId = null;
        ImmutableRoaringBitmap polygonBitmap = null;
        List<Integer> results = new ArrayList<>();
        int productIndex;
        if (startTime == -1) {
            productIndex = 0;
        } else {
            productIndex = getIndexForTime(startTime);
            if (productIndex == -1) {
                return results;
            }
        }

        while (true) {
            if (productIndex >= startTimes.length) {
                break;
            }
            if (useOnlyProductStart) {
                if (endTime != -1 && startTimes[productIndex] >= endTime) {
                    break;
                } else if (startTime != -1 && startTimes[productIndex] < startTime) {
                    // this product starts too early, skip
                    productIndex++;
                    continue;
                }
            } else {
                if (endTime != -1 && startTimes[productIndex] > endTime) {
                    break;
                } else if (startTime != -1 && endTimes[productIndex] < startTime) {
                    // this product ends too early, but the next could be longer
                    productIndex++;
                    continue;
                }
            }

            // time matches, now test geo
            if (point != null) {
                if (s2CellId == null) {
                    s2CellId = S2CellId.fromPoint(point);
                }
                ImmutableRoaringBitmap roaringBitmap = bitmaps[bitmapIndices[productIndex]];
                if (roaringBitmap.contains(S2Integer.asIntAtLevel(s2CellId, maxLevel))) {
                    results.add(productIndex);
                }
            } else if (polygon != null) {
                if (polygonBitmap == null) {
                    polygonBitmap = S2Integer.createCoverageBitmap(polygon, maxLevel);
                }
                ImmutableRoaringBitmap roaringBitmap = bitmaps[bitmapIndices[productIndex]];
                if (ImmutableRoaringBitmap.intersects(roaringBitmap, polygonBitmap)) {
                    results.add(productIndex);
                }
            } else {
                results.add(productIndex);
            }
            productIndex++;
        }
        return results;
    }

    private Collection<String> testPolygonOnData(List<Integer> uniqueProductList, S2Polygon searchPolygon, int numResults) {
        uniqueProductList.sort(Comparator.comparingInt(o -> startTimes[o]));
        List<String> matches = new ArrayList<>();
        try {
            for (Integer productID : uniqueProductList) {
                reader.readEntry(productID);
                if (searchPolygon == null || reader.getCurrentPolygon().intersects(searchPolygon)) {
                    matches.add(reader.getCurrentPath());
                    if (matches.size() == numResults) {
                        return matches;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return matches;
    }

    private List<String> testPointsOnData(Map<Integer, List<S2Point>> candidatesMap, int maxNumResults) {

        List<Integer> uniqueProductList = new ArrayList<>(candidatesMap.keySet());
        uniqueProductList.sort(Comparator.comparingInt(o -> startTimes[o]));

        List<String> matches = new ArrayList<>();
        try {
            for (Integer productID : uniqueProductList) {
                reader.readEntry(productID);

                S2Polygon polygon = reader.getCurrentPolygon();
                List<S2Point> s2Points = candidatesMap.get(productID);
                boolean pointInPolygon = false;
                for (S2Point s2Point : s2Points) {
                    if (polygon.contains(s2Point)) {
                        pointInPolygon = true;
                        break;
                    }
                }
                if (pointInPolygon) {
                    matches.add(reader.getCurrentPath());
                    if (matches.size() == maxNumResults) {
                        return matches;
                    }
                }
            }
            return matches;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return matches;
    }

    private int getIndexForTime(int currentStartTime) {
        return Search.indexedBinarySearch(startTimes, currentStartTime);
    }

}
