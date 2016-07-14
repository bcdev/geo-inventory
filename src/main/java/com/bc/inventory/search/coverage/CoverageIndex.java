package com.bc.inventory.search.coverage;

import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.utils.S2Integer;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
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
public class CoverageIndex {

    static final short MASK_SHIFT = 2 * S2CellId.MAX_LEVEL - 1;

    IndexRecord[] records;
    int[][] allCoverages;


    public static void create(List<CsvRecord> csvRecordList, OutputStream indexOS, OutputStream dataOS) throws IOException {
        List<S2Integer.Coverage> allCoverages = new ArrayList<>();
        try (
                DataOutputStream dosIndex = new DataOutputStream(new BufferedOutputStream(indexOS));
                DataOutputStream dosData = new DataOutputStream(new BufferedOutputStream(dataOS))
        ) {
            int counter = 0;
            dosIndex.writeInt(csvRecordList.size());

            for (CsvRecord csvRecord : csvRecordList) {
                dosIndex.writeLong(csvRecord.getStartTime());
                dosIndex.writeLong(csvRecord.getEndTime());

                S2RegionCoverer coverer = new S2RegionCoverer();
                coverer.setMinLevel(0);
                coverer.setMaxLevel(3);
                coverer.setMaxCells(500);
                S2CellUnion cellUnion = coverer.getCovering(csvRecord.getS2Polygon());

                S2Integer.Coverage s2IntCoverage = new S2Integer.Coverage(cellUnion);
                int index = allCoverages.indexOf(s2IntCoverage);
                if (index <= 0) {
                    allCoverages.add(s2IntCoverage);
                    index = allCoverages.size() - 1;
                }
                dosIndex.writeInt(index);

                // TODO remove, mask is no longer used
                int level1Mask = 0;
                for (int i = 0; i < cellUnion.cellIds().size(); i++) {
                    S2CellId s2CellId = cellUnion.cellIds().get(i);
                    level1Mask |= (1 << (int) (s2CellId.id() >>> MASK_SHIFT));
                }
                dosIndex.writeInt(level1Mask);
                // TODO remove, mask is no longer used

                dosIndex.writeInt(dosData.size());

                writePolygon(csvRecord.getS2Polygon(), dosData);
                dosData.writeUTF(csvRecord.getPath());

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

    private static void writePolygon(S2Polygon s2Polygon, DataOutputStream dos) throws IOException {
        S2Loop loop = s2Polygon.loop(0);
        int numVertices = loop.numVertices();
        dos.writeInt(numVertices);

        for (int i = 0; i < numVertices; i++) {
            S2Point vertex = loop.vertex(i);
            dos.writeDouble(vertex.getX());
            dos.writeDouble(vertex.getY());
            dos.writeDouble(vertex.getZ());
        }

        S2LatLngRect bound = loop.getRectBound();
        double latLo = bound.lat().lo();
        double latHi = bound.lat().hi();
        double lngLo = bound.lng().lo();
        double lngHi = bound.lng().hi();
        dos.writeDouble(latLo);
        dos.writeDouble(latHi);
        dos.writeDouble(lngLo);
        dos.writeDouble(lngHi);

        int firstLogicalVertex = loop.getFirstLogicalVertex();
        dos.writeInt(firstLogicalVertex);
        boolean originInside = loop.isOriginInside();
        dos.writeBoolean(originInside);
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
        final int mask;
        final int dataOffset;

        IndexRecord(long startTime, long endTime, int coverageIndex, int mask, int dataOffset) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.coverageIndex = coverageIndex;
            this.mask = mask;
            this.dataOffset = dataOffset;
        }
    }
}
