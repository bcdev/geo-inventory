package com.bc.inventory.search.ng;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.SimpleRecord;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An inventory based on a list of coverages.
 */
public class NgInventory implements Inventory {

    private final StreamFactory streamFactory;
    private final boolean indexOnly;
    private final String product_ListFilename;
    private final String indexFilename;
    private final String dataFilename;
    private final int maxLevel;

    private CoverageIndex index;

    public NgInventory(String sensor, StreamFactory streamFactory, boolean indexOnly, int maxLevel) {
        this.streamFactory = streamFactory;
        this.indexOnly = indexOnly;
        this.maxLevel = maxLevel;
        product_ListFilename = sensor + "_products_list.csv";
        indexFilename = "ng/" + sensor + "_l" + maxLevel + ".index";
        dataFilename = "ng/" + sensor + "_l" + maxLevel + ".data";
    }

    @Override
    public int createIndex() throws IOException {
        IndexCreator indexCreator = new IndexCreator(maxLevel);
        try (InputStream inputStream = streamFactory.createInputStream(product_ListFilename)) {
            CsvRecordReader.CsvRecordIterator iterator = CsvRecordReader.getIterator(inputStream);
            while (iterator.hasNext()) {
                CsvRecord r = iterator.next();
                indexCreator.addToIndex(r.getPath(), r.getStartTime(), r.getEndTime(), r.getS2Polygon());
            }
        }
        try (OutputStream indexOS = streamFactory.createOutputStream(indexFilename);
             OutputStream dataOS = streamFactory.createOutputStream(dataFilename)) {
            indexCreator.write(indexOS, dataOS);
        }
        return indexCreator.size();
    }

    @Override
    public int loadIndex() throws IOException {
        index = new CoverageIndex(streamFactory.createInputStream(indexFilename));
        return index.size();
    }

    @Override
    public Collection<String> query(Constrain constrain) {
        List<SimpleRecord> insitu = constrain.getInsitu();
        int start = IndexCreator.startTimeInMin(constrain.getStartTime());
        int end = IndexCreator.endTimeInMin(constrain.getEndTime());
        S2Polygon polygon = constrain.getPolygon();

        if (insitu == null) {
            List<Integer> productIDs;
            Collection<String> paths;
            productIDs = testOnIndex(start, end, null, polygon);
            if (indexOnly) {
                paths = testPolygonOnData(productIDs, null, constrain.getNumResults());
            } else {
                paths = testPolygonOnData(productIDs, polygon, constrain.getNumResults());
            }
            return paths;
        } else {
            Map<Integer, List<S2Point>> candidatesMap = new HashMap<>();
            for (SimpleRecord insituRecord : insitu) {
                long delta = constrain.getDelta();
                if (delta != -1) {
                    start = IndexCreator.startTimeInMin(insituRecord.getTime() - delta);
                    end = IndexCreator.endTimeInMin(insituRecord.getTime() + delta);
                }
                S2Point s2Point = insituRecord.getAsPoint();
                List<Integer> productIDs = testOnIndex(start, end, s2Point, null);
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
                paths = testPointsOnData(candidatesMap, constrain.getNumResults());
            }
            return paths;
        }
    }

    private List<Integer> testOnIndex(int startTime, int endTime, S2Point point, S2Polygon polygon) {
        S2CellId s2CellId = null;
        int[] polygonIntIds = null;
        List<Integer> results = new ArrayList<>();
        int productIndex = index.getIndexForTime(startTime);
        if (productIndex == -1) {
            return results;
        }

        boolean finishedWithInsitu = false;
        while (!finishedWithInsitu) {
            if (productIndex >= index.size()) {
                finishedWithInsitu = true;
            } else if (endTime != -1 && index.getStartTime(productIndex) > endTime) {
                finishedWithInsitu = true;
            } else if (startTime != -1 && index.getEndTime(productIndex) < startTime) {
                // this product end too early, but maybe the next will be longer
            } else {
                // time matches, now test geo
                if (point != null) {
                    if (s2CellId == null) {
                        s2CellId = S2CellId.fromPoint(point);
                    }
                    int[] coverage = index.getCoverage(productIndex);
                    if (S2Integer.containsCellId(coverage, s2CellId)) {
                        results.add(productIndex);
                    }
                } else if (polygon != null) {
                    if (polygonIntIds == null) {
                        polygonIntIds = S2Integer.createS2IntIds(polygon, maxLevel);
                    }
                    if (S2Integer.intersectsCellUnionFast(polygonIntIds, index.getCoverage(productIndex))) {
                        results.add(productIndex);
                    }
                } else {
                    results.add(productIndex);
                }
            }
            productIndex++;
        }
        return results;
    }

    private Collection<String> testPolygonOnData(List<Integer> uniqueProductList, S2Polygon searchPolygon, int numResults) {
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(index.getDataOffset(o1), index.getDataOffset(o2)));
        List<String> matches = new ArrayList<>();
        try (
                DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(dataFilename))
        ) {
            for (Integer productID : uniqueProductList) {
                reader.seekTo(index.getDataOffset(productID));
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

    private List<String> testPointsOnData(Map<Integer, List<S2Point>> candidatesMap, int numResults) {

        List<Integer> uniqueProductList = new ArrayList<>(candidatesMap.keySet());
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(index.getDataOffset(o1), index.getDataOffset(o2)));

        List<String> matches = new ArrayList<>();
        try (
                DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(dataFilename))
        ) {
            for (Integer productID : uniqueProductList) {
                reader.seekTo(index.getDataOffset(productID));

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
                    if (matches.size() == numResults) {
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
}
