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
                CsvRecord next = iterator.next();
                if (next != null) {
                    csvRecordList.add(next);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return csvRecordList;
    }

    public static CsvRecordIterator getIterator(InputStream inputStream) throws IOException {
        return new CsvRecordIterator(inputStream);
    }

    public static class CsvRecordIterator implements Iterator<CsvRecord>, AutoCloseable{

        private final BufferedReader bufferedReader;
        private String line;

        private CsvRecordIterator(InputStream inputStream) throws IOException {
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            line = bufferedReader.readLine();
        }

        @Override
        public boolean hasNext() {
            return line != null;
        }

        @Override
        public CsvRecord next() {
            try {
                return parseLine(line);
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            } finally {
                try {
                    line = bufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                    line = null;
                }
            }
        }

        @Override
        public void close() throws IOException {
            bufferedReader.close();
        }
    }

    static CsvRecord parseLine(String line) throws ParseException {
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

    static long parseDateTime(String split) throws ParseException {
        return DATE_FORMAT.parse(DateUtils.getNoFractionString(split)).getTime();
    }

    static S2Polygon parsePolygon(String wkt) {
        return (S2Polygon) WKT_READER.read(wkt);
    }
}
