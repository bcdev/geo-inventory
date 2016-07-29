package com.bc.inventory.search.ng;

import java.io.IOException;
import java.io.InputStream;

/**
 * The index based on coverages
 */
class CoverageIndex {

    private final int[] startTimes;
    private final int[] endTimes;
    private final int[] coverageIndices;
    private final int[] dataOffsets;
    private final int[][] coverages;

    CoverageIndex(InputStream indexIS) throws IOException {
        try (IndexFile.Reader indexFile = new IndexFile.Reader(indexIS)) {
            int numRecords = indexFile.readNumRecords();
            startTimes = indexFile.readIntArray(numRecords);
            endTimes = indexFile.readIntArray(numRecords);
            coverageIndices = indexFile.readIntArray(numRecords);
            dataOffsets = indexFile.readIntArray(numRecords);

            coverages = new int[indexFile.readNumCoverages()][0];
            for (int i = 0; i < coverages.length; i++) {
                coverages[i] = indexFile.readCoverage();
            }
        }
    }

    int size() {
        return startTimes.length;
    }

    int getStartTime(int index) {
        return startTimes[index];
    }

    int getEndTime(int index) {
        return endTimes[index];
    }

    int getDataOffset(int index) {
        return dataOffsets[index];
    }

    int[] getCoverage(int index) {
        return coverages[coverageIndices[index]];
    }

    int getIndexForTime(int currentStartTime) {
        return indexedBinarySearch(startTimes, currentStartTime);
    }

    private static int indexedBinarySearch(int[] startTimes, int currentStartTime) {
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
}
