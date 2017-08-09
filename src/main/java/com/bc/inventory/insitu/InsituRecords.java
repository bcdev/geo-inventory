package com.bc.inventory.insitu;

import com.bc.inventory.utils.SimpleRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

public class InsituRecords {

    public static List<SimpleRecord> read(File file, DateFormat dateFormat) throws IOException {
        try (Reader reader = new LineNumberReader(new FileReader(file), 100 * 1024)) {
            CsvRecordSource recordSource = new CsvRecordSource(reader, dateFormat);
            boolean hasTime = recordSource.getHeader().hasTime();
            List<SimpleRecord> records = new ArrayList<>();
            for (Record record : recordSource.getRecords()) {
                if (hasTime) {
                    records.add(new SimpleRecord(record.getTime().getTime(), record.getLocation()));
                } else {
                    records.add(new SimpleRecord(record.getLocation()));
                }
            }
            return records;
        }
    }

}
