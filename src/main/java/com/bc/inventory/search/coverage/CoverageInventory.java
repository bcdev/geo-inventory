package com.bc.inventory.search.coverage;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.SimpleRecord;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2RegionCoverer;

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
public class CoverageInventory implements Inventory {

    private final String sensor;
    private final StreamFactory streamFactory;
    private final boolean indexOnly;
    private CoverageIndex index;

    public CoverageInventory(String sensor, StreamFactory streamFactory, boolean indexOnly) {
        this.sensor = sensor;
        this.streamFactory = streamFactory;
        this.indexOnly = indexOnly;
    }

    @Override
    public int createIndex() throws IOException {
        try (InputStream inputStream = streamFactory.createInputStream(sensor + "_products_list.csv")) {
            CsvRecordReader csvRecordReader = new CsvRecordReader(inputStream);
            List<CsvRecord> csvRecordList = csvRecordReader.getCsvRecordList();

            OutputStream indexOS = streamFactory.createOutputStream(sensor + "_coverage.index");
            OutputStream dataOS = streamFactory.createOutputStream(sensor + "_coverage.data");
            CoverageIndex.create(csvRecordList, indexOS, dataOS);

            return csvRecordList.size();
        }
    }


    @Override
    public int loadIndex() throws IOException {
        index = new CoverageIndex();
        index.load(streamFactory.createInputStream(sensor + "_coverage.index"));
        return index.records.length;
    }

    @Override
    public Collection<String> query(Constrain constrain) {
        List<SimpleRecord> insitu = constrain.getInsitu();
        long start = constrain.getStart();
        long end = constrain.getEnd();
        S2Polygon polygon = constrain.getPolygon();

        if (insitu == null) {
            List<Integer> productIDs;
            Collection<String> paths;
            productIDs = testOnIndex(start, end, null, polygon);
            if (indexOnly) {
                paths = new ArrayList<>(productIDs.size());
                for (Integer productID : productIDs) {
                    paths.add(Integer.toString(productID));
                }
            } else {
                paths = testPolygonOnData(productIDs, polygon);
            }
            return paths;
        } else {
            Map<Integer, List<S2Point>> candidatesMap = new HashMap<>();
            for (SimpleRecord insituRecord : insitu) {
                long delta = constrain.getDelta();
                if (delta != -1) {
                    start = insituRecord.getTime() - delta;
                    end = insituRecord.getTime() + delta;
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
                    paths.add(Integer.toString(productID));
                }
            } else {
                paths = testPointsOnData(candidatesMap);
            }
            return paths;
        }
    }

    private List<Integer> testOnIndex(long startTime, long endTime, S2Point point, S2Polygon polygon) {
        S2CellId s2CellId = null;
        int[] polygonIntIds = null;
        List<Integer> results = new ArrayList<>();
        int productIndex = index.getIndexForTime(startTime);
        if (productIndex == -1) {
            return Collections.EMPTY_LIST;
        }

        boolean finishedWithInsitu = false;
        while (!finishedWithInsitu) {
            CoverageIndex.IndexRecord record = null;
            if (productIndex < index.records.length) {
                record = index.records[productIndex];
            }

            if (record == null) {
                finishedWithInsitu = true;
            } else if (endTime != -1 && record.startTime > endTime) {
                finishedWithInsitu = true;
            } else if (startTime != -1 && record.endTime < startTime) {
                //test next product;
            } else {
                // time matches, now test geo
                if (point != null) {
                    if (s2CellId == null) {
                        s2CellId = S2CellId.fromPoint(point);
                    }
                    int[] coverage = index.allCoverages[record.coverageIndex];
                    if (S2Integer.containsCellId(coverage, s2CellId)) {
                        results.add(productIndex);
                    }
                } else if (polygon != null) {
                    if (polygonIntIds == null) {
                        S2RegionCoverer coverer = new S2RegionCoverer();
                        coverer.setMinLevel(0);
                        coverer.setMaxLevel(3);
                        coverer.setMaxCells(500);
                        S2CellUnion cellUnion = coverer.getCovering(polygon);
                        polygonIntIds = S2Integer.convertCellUnion(cellUnion);
                    }
                    if (S2Integer.intersectsCellUnionFast(polygonIntIds, index.allCoverages[record.coverageIndex])) {
                        results.add(productIndex);
                    }
                }
            }
            productIndex++;
        }
        return results;
    }

    private Collection<String> testPolygonOnData(List<Integer> uniqueProductList, S2Polygon searchPolygon) {
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(index.records[o1].dataOffset, index.records[o2].dataOffset));
        List<String> matches = new ArrayList<>();
        try (
                DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(sensor + "_coverage.data"))
        ) {
            for (Integer productID : uniqueProductList) {
                CoverageIndex.IndexRecord record = index.records[productID];

                reader.seekTo(record.dataOffset);
                if (reader.readPolygon().intersects(searchPolygon)) {
                    matches.add(reader.readPath());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return matches;
    }

    private List<String> testPointsOnData(Map<Integer, List<S2Point>> candidatesMap) {

        List<Integer> uniqueProductList = new ArrayList<>(candidatesMap.keySet());
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(index.records[o1].dataOffset, index.records[o2].dataOffset));

        List<String> matches = new ArrayList<>();
        try (
                DataFile.Reader reader = new DataFile.Reader(streamFactory.createInputStream(sensor + "_coverage.data"))
        ) {
            for (Integer productID : uniqueProductList) {
                CoverageIndex.IndexRecord record = index.records[productID];
                reader.seekTo(record.dataOffset);

                S2Polygon productPolygon = reader.readPolygon();
                List<S2Point> s2Points = candidatesMap.get(productID);
                boolean readPath = false;
                for (S2Point s2Point : s2Points) {
                    if (productPolygon.contains(s2Point)) {
                        readPath = true;
                        break;
                    }
                }
                if (readPath) {
                    matches.add(reader.readPath());
                }
            }
            return matches;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return matches;
    }
}
