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
import java.util.List;

/**
 *
 */
public class CsvRecordReader {

    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private final List<CsvRecord> csvRecordList;

    public CsvRecordReader(InputStream inputStream) throws IOException {
        S2WKTReader wktReader = new S2WKTReader();
        csvRecordList = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = bufferedReader.readLine();
            while (line != null) {
                try {
                    CsvRecord csvRecord = createRecord(line, wktReader);
                    if (csvRecord != null) {
                        csvRecordList.add(csvRecord);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                line = bufferedReader.readLine();
            }
        }
    }

    public List<CsvRecord> getCsvRecordList() {
        return csvRecordList;
    }

    private CsvRecord createRecord(String line, S2WKTReader wktReader) throws ParseException {
        if (line == null || line.isEmpty()) {
            return null;
        }
        String[] splits = line.split("\t");
        String path = splits[0];
        long startTime = DATE_FORMAT.parse(DateUtils.getNoFractionString(splits[1])).getTime();
        long endTime = DATE_FORMAT.parse(DateUtils.getNoFractionString(splits[2])).getTime();
        String wkt = splits[3];
        S2Polygon s2Polygon = (S2Polygon) wktReader.read(wkt);
        return new CsvRecord(path, startTime, endTime, s2Polygon);
    }

}
