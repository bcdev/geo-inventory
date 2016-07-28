package com.bc.inventory.search.coverage;

import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.utils.S2Integer;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2RegionCoverer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by marcoz on 04.07.16.
 */
class CoverageIndex {

    IndexRecord[] records;
    int[][] allCoverages;


    public static void create(List<CsvRecord> csvRecordList, OutputStream indexOS, OutputStream dataOS) throws IOException {
        List<S2Integer.Coverage> allCoverages = new ArrayList<>();
        try (
                DataOutputStream dosIndex = new DataOutputStream(new BufferedOutputStream(indexOS));
                DataFile.Writer dataWriter = new DataFile.Writer(dataOS);
        ) {
            int counter = 0;
            dosIndex.writeInt(csvRecordList.size());

            for (CsvRecord csvRecord : csvRecordList) {
                dosIndex.writeLong(csvRecord.getStartTime());
                dosIndex.writeLong(csvRecord.getEndTime());

                S2Integer.Coverage s2IntCoverage = S2Integer.createS2IntCoverage(csvRecord.getS2Polygon(), 3);
                int index = allCoverages.indexOf(s2IntCoverage);
                if (index <= 0) {
                    allCoverages.add(s2IntCoverage);
                    index = allCoverages.size() - 1;
                }
                dosIndex.writeInt(index);

                int dataOffset = dataWriter.writeRecord(csvRecord.getS2Polygon(), csvRecord.getPath());
                dosIndex.writeInt(dataOffset);

                counter++;
                if (counter % 10000 == 0) {
                    System.out.println("counter = " + counter);
                }
            }
            dosIndex.writeInt(allCoverages.size());
            for (S2Integer.Coverage s2Cover : allCoverages) {
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
        Arrays.sort(records, (r1, r2) -> Long.compare(r1.startTime, r2.startTime));
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
