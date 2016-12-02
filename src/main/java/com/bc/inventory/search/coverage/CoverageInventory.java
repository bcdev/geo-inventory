package com.bc.inventory.search.coverage;

import com.bc.geometry.s2.S2WKTWriter;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.DateUtils;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.SimpleRecord;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An inventory based on a list of coverages.
 */
public class CoverageInventory implements Inventory {

    private static final long MINUTES_PER_MILLI = 60 * 1000;
    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
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
        return startTimes.length;
    }

    @Override
    public int numEntries() {
        return startTimes != null ? startTimes.length : 0;
    }

    public boolean hasIndex() throws IOException {
        return  streamFactory.exists(INDEX_FILENAME) && streamFactory.exists(DATA_FILENAME);
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
        int[] polygonIntIds = null;
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
                int[] coverage = coverages[coverageIndices[productIndex]];
                if (S2Integer.containsCellId(coverage, s2CellId)) {
                    results.add(productIndex);
                }
            } else if (polygon != null) {
                if (polygonIntIds == null) {
                    polygonIntIds = S2Integer.createS2IntIds(polygon, maxLevel);
                }
                if (S2Integer.intersectsCellUnionFast(polygonIntIds, coverages[coverageIndices[productIndex]])) {
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
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(dataOffsets[o1], dataOffsets[o2]));
        List<String> matches = new ArrayList<>();
        try (DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(DATA_FILENAME))) {
            for (Integer productID : uniqueProductList) {
                reader.seekTo(dataOffsets[productID]);
                if (searchPolygon == null || reader.readPolygon().intersects(searchPolygon)) {
                    matches.add(reader.readPath());
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
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(dataOffsets[o1], dataOffsets[o2]));

        List<String> matches = new ArrayList<>();
        try (DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(DATA_FILENAME))) {
            for (Integer productID : uniqueProductList) {
                reader.seekTo(dataOffsets[productID]);

                S2Polygon productPolygon = reader.readPolygon();
                List<S2Point> s2Points = candidatesMap.get(productID);
                boolean pointInPolygon = false;
                for (S2Point s2Point : s2Points) {
                    if (productPolygon.contains(s2Point)) {
                        pointInPolygon = true;
                        break;
                    }
                }
                if (pointInPolygon) {
                    matches.add(reader.readPath());
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
        return indexedBinarySearch(startTimes, currentStartTime);
    }

    static int indexedBinarySearch(int[] startTimes, int currentStartTime) {
        int low = 0;
        int high = startTimes.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            final int t1 = startTimes[mid];
            if (t1 < currentStartTime) {
                low = mid + 1;
            } else if (t1 == currentStartTime) {
                return mid; // key found
            } else {
                high = mid - 1;
            }
        }
        return low == 0 ? low : low - 1;  // key not found
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
