package com.bc.inventory.search.coverage;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.SimpleRecord;
import com.bc.inventory.utils.StartStopWatch;
import com.google.common.geometry.R1Interval;
import com.google.common.geometry.S1Interval;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2RegionCoverer;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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

    private Collection<String> testPolygonOnData(List<Integer> uniqueProductList, S2Polygon polygon) {
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(index.records[o1].dataOffset, index.records[o2].dataOffset));
        StartStopWatch read = new StartStopWatch();
        StartStopWatch create = new StartStopWatch();
        StartStopWatch test = new StartStopWatch();
        try (
                DataInputStream dis = new DataInputStream(new BufferedInputStream(streamFactory.createInputStream(sensor + "_coverage.data")))
        ) {
            List<String> matches = new ArrayList<>();
            int streamPOS = 0;
            for (Integer productID : uniqueProductList) {
                CoverageIndex.IndexRecord record = index.records[productID];

                read.start();
                dis.skipBytes(record.dataOffset - streamPOS);

                // read polygon
                final int numLoopPoints = dis.readInt();
                final int numLoopBytes = numLoopPoints * 3 * 8 + 4 * 8 + 4 + 1;
                final byte[] loopBytes = new byte[numLoopBytes];
                dis.readFully(loopBytes, 0, numLoopBytes);
                streamPOS = record.dataOffset + 4 + numLoopBytes;
                read.stop();

                create.start();
                S2Polygon productPolygon = createS2Polygon(loopBytes, numLoopPoints);
                create.stop();

                test.start();
                boolean intersects = productPolygon.intersects(polygon);
                test.stop();

                if (intersects) {
                    String path = dis.readUTF();
                    matches.add(path);
                    streamPOS = streamPOS + path.length() + 2;
                }
            }
            System.out.println("read   = " + read.getSum());
            System.out.println("create = " + create.getSum());
            System.out.println("test   = " + test.getSum());
            return matches;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.EMPTY_LIST;
        }
    }

    private List<String> testPointsOnData(Map<Integer, List<S2Point>> candidatesMap) {

        List<Integer> uniqueProductList = new ArrayList<>(candidatesMap.keySet());
        Collections.sort(uniqueProductList, (o1, o2) -> Integer.compare(index.records[o1].dataOffset, index.records[o2].dataOffset));

        try (
                DataInputStream dis = new DataInputStream(new BufferedInputStream(streamFactory.createInputStream(sensor + "_coverage.data")))
        ) {
            List<String> matches = new ArrayList<>();
            int streamPOS = 0;
            for (Integer productID : uniqueProductList) {
                CoverageIndex.IndexRecord record = index.records[productID];
                dis.skipBytes(record.dataOffset - streamPOS);

                // read polygon
                final int numLoopPoints = dis.readInt();
                final int numLoopBytes = numLoopPoints * 3 * 8 + 4 * 8 + 4 +1;
                final byte[] loopBytes = new byte[numLoopBytes];
                dis.readFully(loopBytes, 0, numLoopBytes);
                streamPOS = record.dataOffset + 4 + numLoopBytes;
                S2Polygon productPolygon = createS2Polygon(loopBytes, numLoopPoints);

                List<S2Point> s2Points = candidatesMap.get(productID);
                boolean readPath = false;
                for (S2Point s2Point : s2Points) {
                    if (productPolygon.contains(s2Point)) {
                        readPath = true;
                        break;
                    }
                }
                if (readPath) {
                    String path = dis.readUTF();
                    matches.add(path);
                    streamPOS = streamPOS + path.length() + 2;
                }
            }
            return matches;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.EMPTY_LIST;
        }
    }

    private S2Polygon createS2Polygon(byte[] loopByte, int numLoopPoints) {
        ByteBuffer bb = ByteBuffer.wrap(loopByte);

        S2Point[] vertices = new S2Point[numLoopPoints];
        for (int i = 0; i < numLoopPoints; i++) {
            double x = bb.getDouble();
            double y = bb.getDouble();
            double z = bb.getDouble();
            vertices[i] = new S2Point(x, y, z);
        }
        double latLo = bb.getDouble();
        double latHi = bb.getDouble();
        double lngLo = bb.getDouble();
        double lngHi = bb.getDouble();
        R1Interval lat = new R1Interval(latLo, latHi);
        S1Interval lng = new S1Interval(lngLo, lngHi);
        S2LatLngRect bound = new S2LatLngRect(lat, lng);

        int firstLogicalVertex = bb.getInt();
        boolean originInside = (bb.get() == 1);
        S2Loop loop = new S2Loop(vertices, bound, firstLogicalVertex, originInside);

        return new S2Polygon(loop);
    }
}
