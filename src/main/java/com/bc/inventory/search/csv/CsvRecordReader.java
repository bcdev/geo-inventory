package com.bc.inventory.search.csv;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.collect.Lists;
import com.google.common.geometry.S2Polygon;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class CsvRecordReader {

    private static final DateFormat DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final S2WKTReader WKT_READER = new S2WKTReader();

    public static List<CsvRecord> readAllRecords(InputStream inputStream) {
        try (CsvRecordIterator iterator = getIterator(inputStream)) {
            return Lists.newArrayList(iterator);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.EMPTY_LIST;
    }

    public static CsvRecordIterator getIterator(InputStream inputStream) throws IOException {
        return new CsvRecordIterator(inputStream);
    }

    public static class CsvRecordIterator implements Iterator<CsvRecord>, AutoCloseable {

        private final BufferedReader bufferedReader;
        private boolean reachedEnd;
        private CsvRecord record;

        private CsvRecordIterator(InputStream inputStream) throws IOException {
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            reachedEnd = false;
            record = getNextRecord();
        }

        @Override
        public boolean hasNext() {
            return record != null;
        }

        @Override
        public CsvRecord next() {
            CsvRecord currentRecord = record;
            record = getNextRecord();
            return currentRecord;
        }

        @Override
        public void close() throws IOException {
            bufferedReader.close();
        }

        private CsvRecord getNextRecord() {
            String line = readLineSafe();
            while (!reachedEnd) {
                try {
                    return parseLine(line);
                } catch (ParseException ignore) {
                    ignore.printStackTrace();
                }
                line = readLineSafe();
            }
            return null;
        }

        private String readLineSafe() {
            try {
                String readLine = bufferedReader.readLine();
                if (readLine == null) {
                    reachedEnd = true;
                }
                return readLine;
            } catch (IOException e) {
                e.printStackTrace();
                reachedEnd = true;
            }
            return null;
        }
    }

    private static CsvRecord parseLine(String line) throws ParseException {
        if (line == null || line.isEmpty()) {
            return null;
        }
        String[] splits = line.split("\t");
        if (splits.length < 4) {
            throw new IllegalArgumentException("Can not parse: " + line);
        }
        // make sure: either both dates are given or none
        long startTime = parseDateTime(splits[1]);
        long endTime = parseDateTime(splits[2]);
        if (startTime == -1 && endTime != -1) {
            startTime = endTime;
        } else if (endTime == -1 && startTime != -1) {
            endTime = startTime;
        }
        return new CsvRecord(
                splits[0],
                startTime,
                endTime,
                parsePolygon(splits[3])
        );
    }

    private static long parseDateTime(String split) throws ParseException {
        if (split.isEmpty() || split.equalsIgnoreCase("null")) {
            return -1;
        }
        return DATE_FORMAT.parse(TimeUtils.getNoFractionString(split)).getTime();
    }

    private static S2Polygon parsePolygon(String wkt) {
        return (S2Polygon) WKT_READER.read(wkt);
    }
}
