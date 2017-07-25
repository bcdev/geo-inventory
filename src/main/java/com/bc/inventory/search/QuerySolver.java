package com.bc.inventory.search;

import com.bc.inventory.utils.SimpleRecord;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Evaluates the given Constrain against an Inventory to get to a QueryResult.
 */
public class QuerySolver {
    
    private final Index index;
    private final boolean indexOnly;

    public QuerySolver(Index index) {
        this(index, false);
    }
    
    public QuerySolver(Index index, boolean indexOnly) {
        this.index = index;
        this.indexOnly = indexOnly;
    }
    
    public QueryResult query(Constrain constrain) {
        SimpleRecord[] insituRecords = constrain.getInsituRecords();
        int start = TimeUtils.startTimeInMin(constrain.getStartTime());   // can be -1
        int end = TimeUtils.endTimeInMin(constrain.getEndTime());         // can be -1
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
                    insituStart = TimeUtils.startTimeInMin(insituRecordTime - delta);
                    insituEnd = TimeUtils.endTimeInMin(insituRecordTime + delta);
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
    private List<Integer> testOnIndex(int startTime, int endTime, boolean useOnlyProductStart, S2Point point, S2Polygon
        polygon) {
        S2CellId s2CellId = null;
        ImmutableRoaringBitmap polygonBitmap = null;
        List<Integer> results = new ArrayList<>();
        int productIndex;
        if (startTime == -1) {
            productIndex = 0;
        } else {
            productIndex = index.getIndexForTime(startTime);
            if (productIndex == -1) {
                return results;
            }
        }

        while (true) {
            if (productIndex >= index.size()) {
                break;
            }
            if (useOnlyProductStart) {
                if (endTime != -1 && index.getStartTime(productIndex) >= endTime) {
                    break;
                } else if (startTime != -1 && index.getStartTime(productIndex) < startTime) {
                    // this product starts too early, skip
                    productIndex++;
                    continue;
                }
            } else {
                if (endTime != -1 && index.getStartTime(productIndex) > endTime) {
                    break;
                } else if (startTime != -1 && index.getEndTime(productIndex) < startTime) {
                    // this product ends too early, but the next could be longer
                    productIndex++;
                    continue;
                }
            }

            // time matches, now test geo
            if (point != null) {
                if (index.containsPoint(productIndex, point)) {
                    results.add(productIndex);
                }
            } else if (polygon != null) {
                if (index.intersectsPolygon(productIndex, polygon)) {
                    results.add(productIndex);
                }
            } else {
                results.add(productIndex);
            }
            productIndex++;
        }
        return results;
    }

    private Collection<String> testPolygonOnData( List<Integer> uniqueProductList, S2Polygon searchPolygon, int numResults) {
            
        uniqueProductList.sort(Comparator.comparingInt(index::getStartTime));
        List<String> matches = new ArrayList<>();
        try {
            for (Integer productID : uniqueProductList) {
                index.readEntry(productID);
                if (searchPolygon == null || index.getCurrentPolygon().intersects(searchPolygon)) {
                    matches.add(index.getCurrentPath());
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

    private List<String> testPointsOnData(Map<Integer, List<S2Point >> candidatesMap, int maxNumResults) {

        List<Integer> uniqueProductList = new ArrayList<>(candidatesMap.keySet());
        uniqueProductList.sort(Comparator.comparingInt(index::getStartTime));

        List<String> matches = new ArrayList<>();
        try {
            for (Integer productID : uniqueProductList) {
                index.readEntry(productID);

                S2Polygon polygon = index.getCurrentPolygon();
                List<S2Point> s2Points = candidatesMap.get(productID);
                boolean pointInPolygon = false;
                for (S2Point s2Point : s2Points) {
                    if (polygon.contains(s2Point)) {
                        pointInPolygon = true;
                        break;
                    }
                }
                if (pointInPolygon) {
                    matches.add(index.getCurrentPath());
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
    
}
