package com.bc.inventory.search.compressed;

import com.bc.inventory.utils.S2Utils;
import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2Polygon;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Creates or updates a coverage index
 */
class IndexCreator {

    private final int maxLevel;
    private final List<S2Integer.Coverage> bitmapList;
    private final Map<S2Integer.Coverage, Integer> bitmapMap;
    private final List<Entry> indexRecords;
    private final Set<String> allPaths;

    public IndexCreator(int maxLevel) {
        this.maxLevel = maxLevel;
        indexRecords = new ArrayList<>();
        bitmapList = new ArrayList<>();
        bitmapMap = new HashMap<>();
        allPaths = new HashSet<>();
    }

    public void loadExistingIndex(ImageInputStream iis) throws IOException {
//        try (DbFileCov2.Reader reader = new DbFileCov2.Reader(iis)) {
//            reader.readIndex();
//            int[] startTimes = reader.getStartTimes();
//            int[] endTimes = reader.getEndTimes();
//
//            for (int i = 0; i < reader.numBitmaps(); i++) {
//                ImmutableRoaringBitmap bitmap = reader.getBitmap(i);
//                bitmapList.add(bitmap);
//                int index = bitmapList.size() - 1;
//                bitmapMap.put(bitmap, index);
//            }
//            for (int i = 0; i < startTimes.length; i++) {
//                reader.readEntry(i);
//                String path = reader.getCurrentPath();
//                byte[] polygonBytes = reader.getCurrentPolygonBytes();
//                indexRecords.add(new Entry(startTimes[i], endTimes[i], reader.getBitmapIndex(i), path, polygonBytes));
//            }
//        }
    }

    public void addToIndex(String path, long startTime, long endTime, S2Polygon s2Polygon) throws IOException {
        S2CellUnion s2CellUnion = S2Integer.createCellUnion(s2Polygon, maxLevel);
        S2Integer.Coverage s2IntCoverage = new S2Integer.Coverage(S2Integer.cellUnion2Ints(s2CellUnion));
        int coverageId = getUniqeCoverageId(s2IntCoverage);
        byte[] polygonBytes = S2Utils.asBytes(s2Polygon);

        if (!allPaths.contains(path)) {
            Entry record = new Entry(TimeUtils.startTimeInMin(startTime), TimeUtils.endTimeInMin(endTime), coverageId, path,
                                     polygonBytes);
            indexRecords.add(record);
            allPaths.add(path);
        }
    }

    public void removeFromIndex(String path) {
        for (int i = 0; i < indexRecords.size(); i++) {
            Entry entry = indexRecords.get(i);
            if (entry.path.equals(path)) {
                indexRecords.remove(i);
            }
        }
        allPaths.remove(path);
    }

    private int getUniqeCoverageId(S2Integer.Coverage s2IntCoverage) {
        Integer index = bitmapMap.get(s2IntCoverage);
        if (index == null) {
            bitmapList.add(s2IntCoverage);
            index = bitmapList.size() - 1;
            bitmapMap.put(s2IntCoverage, index);
        }
        return index;
    }

    public void write(OutputStream os) throws IOException {
        indexRecords.sort(Comparator.comparingInt(r -> r.startTime));
        try(DbFile.Writer writer = new DbFile.Writer(os)) {
            writer.write(indexRecords, bitmapList);
        }
    }

    public int size() {
        return indexRecords.size();
    }
}
