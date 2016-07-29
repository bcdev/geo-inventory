package com.bc.inventory.search.ng;

import java.io.IOException;
import java.io.InputStream;

/**
 * The index based on coverages
 */
class CoverageIndex {

    private final IndexRecord[] records;
    private final int[][] coverages;

    CoverageIndex(InputStream indexIS) throws IOException {
        try (IndexFile.Reader indexFile = new IndexFile.Reader(indexIS)) {
            records = new IndexRecord[indexFile.readNumRecords()];
            for (int i = 0; i < records.length; i++) {
                records[i] = indexFile.readIndexRecord();
            }
            coverages = new int[indexFile.readNumRecords()][0];
            for (int i = 0; i < coverages.length; i++) {
                coverages[i] = indexFile.readCoverage();
            }
        }
    }

    int size() {
        return records.length;
    }

    CoverageIndex.IndexRecord getRecord(int index) {
        return records[index];
    }

    int[] getCoverage(int index) {
        return coverages[index];
    }

    int getIndexForTime(int startTime) {
        return indexedBinarySearch(records, startTime);
    }

    private static int indexedBinarySearch(IndexRecord[] records, int startTime) {
        int low = 0;
        int high = records.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            final int t1 = records[mid].startTime;
            if (t1 < startTime) {
                low = mid + 1;
            } else if (t1 == startTime) {
                return mid; // key found
            } else {
                high = mid - 1;
            }
        }
        return low == 0 ? low : low - 1;  // key not found
    }

    static class IndexRecord {
        final int startTime;
        final int endTime;
        final int coverageIndex;
        final int dataOffset;

        IndexRecord(int startTime, int endTime, int coverageIndex, int dataOffset) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.coverageIndex = coverageIndex;
            this.dataOffset = dataOffset;
        }
    }
}
