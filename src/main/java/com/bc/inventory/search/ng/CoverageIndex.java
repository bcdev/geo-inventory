package com.bc.inventory.search.ng;

import com.bc.inventory.utils.S2Integer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by marcoz on 04.07.16.
 */
class CoverageIndex {

    IndexRecord[] records;
    int[][] allCoverages;

    public static void write(List<IndexCreator.IndexRecord> recordList, List<S2Integer.Coverage> coverages, OutputStream indexOS, OutputStream dataOS) throws IOException {
        try (
                DataOutputStream dosIndex = new DataOutputStream(new BufferedOutputStream(indexOS));
                DataFile.Writer dataWriter = new DataFile.Writer(dataOS);
        ) {
            int counter = 0;
            dosIndex.writeInt(recordList.size());

            for (IndexCreator.IndexRecord record : recordList) {
                dosIndex.writeLong(record.startTime);
                dosIndex.writeLong(record.endTime);
                dosIndex.writeInt(record.coverageId);

                int dataOffset = dataWriter.writeRecord(record.polygonBytes, record.path);
                dosIndex.writeInt(dataOffset);

                counter++;
                if (counter % 10000 == 0) {
                    System.out.println("counter = " + counter);
                }
            }
            dosIndex.writeInt(coverages.size());
            for (S2Integer.Coverage s2Cover : coverages) {
                int[] intIds = s2Cover.intIds;
                dosIndex.writeInt(intIds.length);
                for (int intId : intIds) {
                    dosIndex.writeInt(intId);
                }
            }
        }
    }

    public void load(InputStream indexIS) throws IOException {
        try (
                DataInputStream disIndex = new DataInputStream(new BufferedInputStream(indexIS));
        ) {
            int numProducts = disIndex.readInt();
            records = new IndexRecord[numProducts];
            for (int i = 0; i < records.length; i++) {
                records[i] = new IndexRecord(
                        disIndex.readLong(),
                        disIndex.readLong(),
                        disIndex.readInt(),
                        disIndex.readInt()
                );
            }
            int numCovers = disIndex.readInt();
            allCoverages = new int[numCovers][0];
            for (int i = 0; i < allCoverages.length; i++) {
                int numCells = disIndex.readInt();
                allCoverages[i] = new int[numCells];
                for (int j = 0; j < allCoverages[i].length; j++) {
                    allCoverages[i][j] = disIndex.readInt();
                }
            }
        }
    }

    public int getIndexForTime(long startTime) {
        return indexedBinarySearch(records, startTime);
    }

    static int indexedBinarySearch(IndexRecord[] records, long startTime) {
        int low = 0;
        int high = records.length -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            final long t1 = records[mid].startTime;
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
        final long startTime;
        final long endTime;
        final int coverageIndex;
        final int dataOffset;

        IndexRecord(long startTime, long endTime, int coverageIndex, int dataOffset) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.coverageIndex = coverageIndex;
            this.dataOffset = dataOffset;
        }
    }
}
