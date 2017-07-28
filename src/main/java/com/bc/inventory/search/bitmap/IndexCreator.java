package com.bc.inventory.search.bitmap;

import com.bc.inventory.utils.S2Integer;
import com.bc.inventory.utils.S2Utils;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.geometry.S2Polygon;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 * Creates or updates a coverage index
 */
class IndexCreator {

    private final int maxLevel;
    private final List<ImmutableRoaringBitmap> bitmapList;
    private final Map<ImmutableRoaringBitmap, Integer> bitmapMap;
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
        try (DbFile.Reader reader = new DbFile.Reader(iis)) {
            reader.readIndex();
            int[] startTimes = reader.getStartTimes();
            int[] endTimes = reader.getEndTimes();

            for (int i = 0; i < reader.numBitmaps(); i++) {
                ImmutableRoaringBitmap bitmap = reader.getBitmap(i);
                bitmapList.add(bitmap);
                int index = bitmapList.size() - 1;
                bitmapMap.put(bitmap, index);
            }
            for (int i = 0; i < startTimes.length; i++) {
                reader.readEntry(i);
                String path = reader.getCurrentPath();
                byte[] polygonBytes = reader.getCurrentPolygonBytes();
                indexRecords.add(new Entry(startTimes[i], endTimes[i], reader.getBitmapIndex(i), path, polygonBytes));
            }
        }
    }

    public void addToIndex(String path, long startTime, long endTime, S2Polygon s2Polygon) throws IOException {
        ImmutableRoaringBitmap coverageBitmap = S2Integer.createCoverageBitmap(s2Polygon, maxLevel);
        int coverageId = getUniqeCoverageId(coverageBitmap);
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

    private int getUniqeCoverageId(ImmutableRoaringBitmap roaringBitmap) {
        Integer index = bitmapMap.get(roaringBitmap);
        if (index == null) {
            bitmapList.add(roaringBitmap);
            index = bitmapList.size() - 1;
            bitmapMap.put(roaringBitmap, index);
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
