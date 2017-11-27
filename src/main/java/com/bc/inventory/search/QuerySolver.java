package com.bc.inventory.search;

import com.bc.inventory.utils.SimpleRecord;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;
import java.util.*;

/**
 * Evaluates the given Constrain against an Inventory to get to a QueryResult.
 */
public class QuerySolver {
    
    private final GeoIndex index;

    public QuerySolver(GeoIndex index) {
        this.index = index;
    }
    
    public List<String> query(Constrain constrain) throws IOException {
        SimpleRecord[] insituRecords = constrain.getInsituRecords();
        int maxNumResults = constrain.getMaxNumResults();

        if (insituRecords.length == 0) {
            S2Polygon polygon = constrain.getPolygon();
            boolean useOnlyProductStart = constrain.useOnlyProductStart();
            Constrain.DateRange[] dateRanges = constrain.getDateRanges();
            List<Integer> productIDs = null;
            Set<Integer> uniqueProductIds = new HashSet<>();
            for (Constrain.DateRange dateRange : dateRanges) {
                int start = TimeUtils.startTimeInMin(dateRange.getStart());   // can be -1
                int end = TimeUtils.endTimeInMin(dateRange.getEnd());         // can be -1
                List<Integer> someProductIDs = testOnIndex(start, end, useOnlyProductStart, null, polygon);
                if (dateRanges.length > 1) {
                    uniqueProductIds.addAll(someProductIDs);
                } else {
                    productIDs = someProductIDs;
                }
            }
            if (dateRanges.length > 1) {
                productIDs = new ArrayList<>(uniqueProductIds);
            }
            return testPolygonOnData(productIDs, polygon, maxNumResults);
        } else {
            Map<Integer, List<S2Point>> candidatesMap = new HashMap<>();
            for (Constrain.DateRange dateRange : constrain.getDateRanges()) {
                int start = TimeUtils.startTimeInMin(dateRange.getStart());   // can be -1
                int end = TimeUtils.endTimeInMin(dateRange.getEnd());         // can be -1

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
                            candidatesMap.computeIfAbsent(match, k -> new ArrayList<>()).add(s2Point);
                        }
                    }
                }
            }
            return testPointsOnData(candidatesMap, maxNumResults);
        }
    }

    private List<Integer> testOnIndex(int startTime, int endTime, boolean useOnlyProductStart, 
                                      S2Point point, S2Polygon polygon) {
        List<Integer> results = new ArrayList<>();
        int productIndex = 0;
        while(productIndex < index.size() && index.getStartTime(productIndex) == -1) {
            checkGeoApproximation(point, polygon, results, productIndex);
            productIndex++;
        }
        if (startTime != -1) {
            productIndex = index.getIndexForTime(startTime);
            if (productIndex == -1) {
                return results;
            }
        }

        while (productIndex < index.size()) {
            if (endTime != -1 && index.getStartTime(productIndex) >= endTime) {
                break;
            } else if (startTime != -1 && (useOnlyProductStart ? index.getStartTime(productIndex) : index.getEndTime(productIndex)) < startTime) {
                // this product starts or ends too early, skip
                productIndex++;
                continue;
            }

            // time matches, now test geo
            checkGeoApproximation(point, polygon, results, productIndex);
            productIndex++;
        }
        return results;
    }

    private void checkGeoApproximation(S2Point point, S2Polygon polygon, List<Integer> results, int productIndex) {
        if (point != null) {
            if (index.approximationContainsPoint(productIndex, point)) {
                results.add(productIndex);
            }
        } else if (polygon != null) {
            if (index.approximationIntersectsPolygon(productIndex, polygon)) {
                results.add(productIndex);
            }
        } else {
            results.add(productIndex);
        }
    }

    private List<String> testPolygonOnData( List<Integer> uniqueProductList, S2Polygon searchPolygon, int numResults) throws IOException {
        Integer[] uniqueProductIDs = uniqueProductList.toArray(new Integer[0]);
        Arrays.sort(uniqueProductIDs, Integer::compare);

        List<String> matches = new ArrayList<>();
        for (Integer productID : uniqueProductIDs) {
            index.readEntry(productID);
            if (searchPolygon == null || index.getCurrentPolygon().intersects(searchPolygon)) {
                matches.add(index.getCurrentPath());
                if (matches.size() == numResults) {
                    return matches;
                }
            }
        }
        return matches;
    }

    private List<String> testPointsOnData(Map<Integer, List<S2Point >> candidatesMap, int maxNumResults) throws IOException {
        Integer[] uniqueProductIDs = candidatesMap.keySet().toArray(new Integer[0]);
        Arrays.sort(uniqueProductIDs, Integer::compareTo);

        List<String> matches = new ArrayList<>();
        for (Integer productID : uniqueProductIDs) {
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
    }
}
