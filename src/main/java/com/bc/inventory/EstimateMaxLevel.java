package com.bc.inventory;

import com.bc.inventory.search.csv.CsvRecord;
import com.bc.inventory.search.csv.CsvRecordReader;
import com.bc.inventory.utils.S2Integer;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2Polygon;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Tests which maxLevel is good
 */
public class EstimateMaxLevel {

    // 15 is maxLevel to be encoded as an INTEGER
    private static final int MAX_LEVEL = 15;

    public static void main(String[] args) throws IOException {
        final File baseDir = new File(args[0]);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            System.out.printf("args[0] = '%s' is not a directory%n", args[0]);
            System.exit(1);
        }
        File[] files = baseDir.listFiles(file -> file.exists() && file.getName().endsWith("_products_list.csv"));
        if (files != null) {
            for (File file : files) {
                Stats stats = new Stats();
                System.out.println("file = " + file);
                try (InputStream inputStream = new FileInputStream(file)) {
                    CsvRecordReader.CsvRecordIterator iterator = CsvRecordReader.getIterator(inputStream);
                    int counter = 0;
                    while (iterator.hasNext()) {
                        CsvRecord r = iterator.next();
                        S2Polygon s2Polygon = r.getS2Polygon();
                        double s2PolygonArea = s2Polygon.getArea();
                        if (s2PolygonArea > 0.0000001) {
                            handle(s2Polygon, stats);
                            counter++;
                            if (counter == 100) {
                                break;
                            }
                        }
                    }
                }
                System.out.println(stats);
            }
        }
    }

    private static void handle(S2Polygon s2Polygon, Stats stats) {
        double polygonArea = s2Polygon.getArea();
//        int level = 5;
        for (int level = 1; level < MAX_LEVEL; level++) {
            S2CellUnion cellUnion = S2Integer.createCellUnion(s2Polygon, level);
            double coverageArea = cellUnion.exactArea();
//            System.out.printf("coverageArea = %.6f  polygonArea = %.6f  ratio = %10.3f%n", coverageArea, polygonArea, coverageArea / polygonArea);
            ImmutableRoaringBitmap bitmap = S2Integer.createCoverageBitmap(cellUnion, level);
            double ratio = coverageArea / polygonArea;
            stats.add(level, ratio, bitmap.serializedSizeInBytes());
            if (ratio < 1.2) {
                break;
            }
        }
    }

    private static class Stats {
        private final Stat[] stats1;
        private final Stat[] stats2;

        private Stats() {
            this.stats1 = new Stat[MAX_LEVEL];
            this.stats2 = new Stat[MAX_LEVEL];
            for (int i = 1; i < MAX_LEVEL; i++) {
                stats1[i] = new Stat();
                stats2[i] = new Stat();
            }
        }

        public void add(int level, double value1, double value2) {
            stats1[level].add(value1);
            stats2[level].add(value2);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int level = 1; level < MAX_LEVEL; level++) {
                if (stats1[level].count>0) {
                    String s1 = stats1[level].toString();
                    String s2 = stats2[level].toString();
                    sb.append(String.format("level %2d   %s  %s%n", level, s1, s2));
                }
            }
            return sb.toString();
        }
    }

    private static class Stat {

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum = 0;
        private int count = 0;

        private void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        @Override
        public String toString() {
            return String.format("min %12.3f   max %12.3f   mean %12.3f   count %3d", min, max, (sum / count), count);
        }
    }
}
