package com.bc.inventory.search.ng;

import com.bc.inventory.utils.S2Integer;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates or updates a coverage index
 */
class IndexCreator {

    private final int maxLevel;
    final List<S2Integer.Coverage> coverages;
    final List<IndexRecord> indexRecords;

    public IndexCreator(int maxLevel) {
        this.maxLevel = maxLevel;
        indexRecords = new ArrayList<>();
        coverages = new ArrayList<>();
    }

    public void loadExistingIndex() {
        // TODO
    }

    public void addToIndex(String path, long startTime, long endTime, S2Polygon s2Polygon) throws IOException {
        S2Integer.Coverage s2IntCoverage = S2Integer.createS2IntCoverage(s2Polygon, maxLevel);
        int coverageId = getUniqCoverageId(s2IntCoverage);
        byte[] polygonBytes = DataFile.polygon2byte(s2Polygon);
        indexRecords.add(new IndexRecord(path, startTime, endTime, coverageId, polygonBytes));
        if (indexRecords.size() % 10000 == 0) {
            System.out.println("#indexRecords: " + indexRecords.size());
        }
    }

    public void removeFromIndex(String path) {
        // TODO
    }

    private int getUniqCoverageId(S2Integer.Coverage s2IntCoverage) {
        int index = coverages.indexOf(s2IntCoverage);
        if (index <= 0) {
            coverages.add(s2IntCoverage);
            index = coverages.size() - 1;
        }
        return index;
    }

    public void debug() {
        System.out.println("maxLevel = " + maxLevel);
        System.out.println("coverages = " + coverages.size());
        System.out.println("indexRecords = " + indexRecords.size());
    }

    static class IndexRecord {
        final String path;
        final long startTime;
        final long endTime;
        final int coverageId;
        final byte[] polygonBytes;

        IndexRecord(String path, long startTime, long endTime, int coverageId, byte[] polygonBytes) {
            this.path = path;
            this.startTime = startTime;
            this.endTime = endTime;
            this.coverageId = coverageId;
            this.polygonBytes = polygonBytes;
        }
    }
}
