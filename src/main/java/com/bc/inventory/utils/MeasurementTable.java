package com.bc.inventory.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by marcoz on 04.07.16.
 */
public class MeasurementTable {

    private final List<Measurement> data = new ArrayList<>();
    private final String title;

    public MeasurementTable(String title) {
        this.title = title;
    }

    public void addMeasurement(Measurement measurement) {
        data.add(measurement);
    }

    public void printMeasurements() {
        int lengthName = 0;
        List<String> engines = new ArrayList<>();
        List<String> testNames = new ArrayList<>();
        for (Measurement m : data) {
            lengthName = Math.max(lengthName, m.testName.length());
            if (!engines.contains(m.engine)) {
                engines.add(m.engine);
            }
            if (!testNames.contains(m.testName)) {
                testNames.add(m.testName);
            }
        }
        String formatHeader1 = "%-" + lengthName + "s";
        String formatHeader2 = "%-" + lengthName + "s";
        String formatTest = "%-" + lengthName + "s";
        String formatRecord = " | %10d  %10d ms";
        String formatNoRecord = " |                          ";
        StringBuilder header3 = new StringBuilder();
        for (int i = 0; i < lengthName; i++) {
             header3.append('-');
        }
        for (String engine : engines) {
            formatHeader1 += " | %15s          ";
            formatHeader2 += " | %10s  %10s   ";
            header3.append("-+--------------------------");
        }
        formatHeader1 += "\n";
        formatHeader2 += "\n";

        System.out.println();
        System.out.println();
        System.out.println("             " +  title);
        System.out.println();

        engines.add(0, "");
        System.out.format(formatHeader1, engines.toArray(new String[0]));
        engines.remove(0);

        String[] header2 = new String[engines.size()*2+1];
        header2[0] = "test";
        for (int i = 1; i < header2.length;) {
            header2[i++] = "products";
            header2[i++] = "time";
        }
        System.out.format(formatHeader2, header2);

        System.out.println(header3);

        for (String testName : testNames) {
            System.out.format(formatTest, testName);
            for(String engine : engines) {
                boolean printed = false;
                for (Measurement m : data) {
                    if (testName.equals(m.testName) && engine.equals(m.engine)) {
                        System.out.format(formatRecord, m.numProducts, m.ms);
                        printed = true;
                        break;
                    }
                }
                if (!printed) {
                    System.out.format(formatNoRecord);
                }
            }
            System.out.println();
        }
    }
}
