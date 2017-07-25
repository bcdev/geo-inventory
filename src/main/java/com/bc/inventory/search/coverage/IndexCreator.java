package com.bc.inventory.search.coverage;

import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2Polygon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Creates or updates a coverage index
 */
class IndexCreator {

    private final int maxLevel;
    private final List<S2Integer.Coverage> coverages;
    private final List<IndexRecord> indexRecords;
    private final Set<String> allPaths;

    public IndexCreator(int maxLevel) {
        this.maxLevel = maxLevel;
        indexRecords = new ArrayList<>();
        coverages = new ArrayList<>();
        allPaths = new HashSet<>();
    }

    public void loadExistingIndex(InputStream indexIS, InputStream dataIS) throws IOException {
        try (IndexFile.Reader indexFile = new IndexFile.Reader(indexIS)) {
            indexFile.readRecords();
            int[] startTimes = indexFile.getStartTimes();
            int[] endTimes = indexFile.getEndTimes();
            int[] coverageIndices = indexFile.getCoverageIndices();
            int[] dataOffsets = indexFile.getDataOffsets();
            for (int i = 0; i < startTimes.length; i++) {
                indexRecords.add(new IndexRecord(startTimes[i], endTimes[i], coverageIndices[i], dataOffsets[i]));
            }

            int[][] coverageArrays = indexFile.readCoverages();
            for (int[] coverageArray : coverageArrays) {
                coverages.add(new S2Integer.Coverage(coverageArray));
            }
        }
        Collections.sort(indexRecords, (r1, r2) -> Integer.compare(r1.dataOffset, r2.dataOffset));
        try (DataFile.Reader reader = new DataFile.Reader(dataIS)) {
            for (IndexRecord indexRecord : indexRecords) {
                reader.seekTo(indexRecord.dataOffset);
                indexRecord.polygonBytes = reader.readPolygonBytes();
                indexRecord.path = reader.readPath();
                allPaths.add(indexRecord.path);
            }
        }
    }

    public void addToIndex(String path, long startTime, long endTime, S2Polygon s2Polygon) throws IOException {
        S2CellUnion s2CellUnion = S2Integer.createCellUnion(s2Polygon, maxLevel);
        S2Integer.Coverage s2IntCoverage = new S2Integer.Coverage(S2Integer.cellUnion2Ints(s2CellUnion));
        int coverageId = getUniqeCoverageId(s2IntCoverage);
        byte[] polygonBytes = DataFile.polygon2byte(s2Polygon);

        if (!allPaths.contains(path)) {
            IndexRecord record = new IndexRecord(path,
                                                 TimeUtils.startTimeInMin(startTime),
                                                 TimeUtils.endTimeInMin(endTime),
                                                 coverageId,
                                                 polygonBytes);
            indexRecords.add(record);
            allPaths.add(path);
        }
    }

    public void removeFromIndex(String path) {
        for (int i = 0; i < indexRecords.size(); i++) {
            IndexRecord indexRecord = indexRecords.get(i);
            if (indexRecord.path.equals(path)) {
                indexRecords.remove(i);
            }
        }
        allPaths.remove(path);
    }

    private int getUniqeCoverageId(S2Integer.Coverage s2IntCoverage) {
        int index = coverages.indexOf(s2IntCoverage);
        if (index <= 0) {
            coverages.add(s2IntCoverage);
            index = coverages.size() - 1;
        }
        return index;
    }

    public void write(OutputStream indexOS, OutputStream dataOS) throws IOException {
        Collections.sort(indexRecords, (r1, r2) -> Integer.compare(r1.startTime, r2.startTime));

        try (DataFile.Writer dataWriter = new DataFile.Writer(dataOS)) {
            for (IndexCreator.IndexRecord record : indexRecords) {
                record.dataOffset = dataWriter.writeRecord(record.polygonBytes, record.path);
            }
        }
        try (IndexFile.Writer indexWriter = new IndexFile.Writer(indexOS)) {
            indexWriter.writeRecords(indexRecords);
            indexWriter.writeCoverages(coverages);
        }
    }

    public int size() {
        return indexRecords.size();
    }

    static class IndexRecord {
        String path;
        final int startTime;
        final int endTime;
        final int coverageId;
        byte[] polygonBytes;
        int dataOffset;

        private IndexRecord(String path, int startTime, int endTime, int coverageId, byte[] polygonBytes) {
            this.path = path;
            this.startTime = startTime;
            this.endTime = endTime;
            this.coverageId = coverageId;
            this.polygonBytes = polygonBytes;
        }

        private IndexRecord(int startTime, int endTime, int coverageId, int dataOffset) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.coverageId = coverageId;
            this.dataOffset = dataOffset;
        }
    }
    

}
