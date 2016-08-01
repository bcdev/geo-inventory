package com.bc.inventory.search.csv;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.inventory.utils.DateUtils;
import com.google.common.geometry.S2Polygon;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class CsvRecordReader {

    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final S2WKTReader WKT_READER = new S2WKTReader();

    public static List<CsvRecord> readAllRecords(InputStream inputStream) {
        List<CsvRecord> csvRecordList = new ArrayList<>();
        try (
                CsvRecordIterator iterator = getIterator(inputStream)
        ) {
            while (iterator.hasNext()) {
                csvRecordList.add(iterator.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return csvRecordList;
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
            record = null;
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
            while (reachedEnd) {
                try {
                    record = parseLine(line);
                } catch (ParseException ignore) {
                    ignore.printStackTrace();
                } finally {
                    line = readLineSafe();
                }
            }
            return record;
        }

        private String readLineSafe() {
            try {
                String readLine = bufferedReader.readLine();
                if (readLine == null) {
                    record = null;
                    reachedEnd = true;
                }
                return readLine;
            } catch (IOException e) {
                e.printStackTrace();
                record = null;
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
        return new CsvRecord(
                splits[0],
                parseDateTime(splits[1]),
                parseDateTime(splits[2]),
                parsePolygon(splits[3])
        );
    }

    private static long parseDateTime(String split) throws ParseException {
        return DATE_FORMAT.parse(DateUtils.getNoFractionString(split)).getTime();
    }

    private static S2Polygon parsePolygon(String wkt) {
        return (S2Polygon) WKT_READER.read(wkt);
    }
}
