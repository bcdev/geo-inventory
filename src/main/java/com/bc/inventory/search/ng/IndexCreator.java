package com.bc.inventory.search.ng;

import com.bc.inventory.utils.S2Integer;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Creates or updates a coverage index
 */
class IndexCreator {

    private static final double MINUTES_PER_MILLI = 60.0 * 1000;

    private final int maxLevel;
    private final List<S2Integer.Coverage> coverages;
    private final List<IndexRecord> indexRecords;

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

        indexRecords.add(new IndexRecord(path, startTimeInMin(startTime), endTimeInMin(endTime), coverageId, polygonBytes));

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

    void write(OutputStream indexOS, OutputStream dataOS) throws IOException {
        Collections.sort(indexRecords, (r1, r2) -> Long.compare(r1.startTime, r2.startTime));

        try (DataFile.Writer dataWriter = new DataFile.Writer(dataOS)) {
            for (IndexCreator.IndexRecord record : indexRecords) {
                record.dataOffset = dataWriter.writeRecord(record.polygonBytes, record.path);
            }
        }
        try (IndexFile.Writer indexWriter = new IndexFile.Writer(indexOS)) {
            indexWriter.writeNumRecords(indexRecords.size());
            for (IndexCreator.IndexRecord record : indexRecords) {
                indexWriter.writeRecord(record.startTime, record.endTime, record.coverageId, record.dataOffset);
            }
            indexWriter.writeNumRecords(coverages.size());
            for (S2Integer.Coverage s2Cover : coverages) {
                indexWriter.writeCoverage(s2Cover.intIds);
            }
        }
    }

    int size() {
        return indexRecords.size();
    }

    private static class IndexRecord {
        final String path;
        final int startTime;
        final int endTime;
        final int coverageId;
        final byte[] polygonBytes;
        int dataOffset;

        private IndexRecord(String path, int startTime, int endTime, int coverageId, byte[] polygonBytes) {
            this.path = path;
            this.startTime = startTime;
            this.endTime = endTime;
            this.coverageId = coverageId;
            this.polygonBytes = polygonBytes;
        }
    }

    static int startTimeInMin(long startTime) {
        if (startTime == -1) {
            return -1;
        }
        return (int) Math.floor(startTime / MINUTES_PER_MILLI);
    }

    static int endTimeInMin(long endTime) {
        if (endTime == -1) {
            return -1;
        }
        return (int) Math.ceil(endTime / MINUTES_PER_MILLI);
    }

}
