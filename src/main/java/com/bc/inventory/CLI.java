package com.bc.inventory;

import com.bc.inventory.insitu.InsituRecords;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.Inventory;
import com.bc.inventory.search.FileStreamFactory;
import com.bc.inventory.search.SafeUpdateInventory;
import com.bc.inventory.search.SimpleInventory;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.utils.SimpleRecord;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * A command line interface to the geo inventory.
 * The following options are supported:
 * <p>
 * update <DB-dir> <CSV-path>
 * creates or updates the DB from the given CSV file
 * <p>
 * dump <DB-dir> <CSV-path>
 * writes the content of the DB to the CSV file
 * <p>
 * query <DB-dir> <constraints>
 * queries the DB using the given constraints.
 */
public class CLI {

    private static final long HOURS_IN_MILLIS = 1000 * 60 * 60; // Note: time in ms (NOT h)

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }
        String mode = args[0].toLowerCase();
        String dbDIR = args[1];
        Inventory inventory = createInventory(new FileStreamFactory(), dbDIR);
        switch (mode) {
            case "update":
                update(inventory, args[2]);
                System.exit(0);
            case "query":
                query(inventory, args);
                System.exit(0);
            case "dump":
                dump(inventory, args[2]);
                System.exit(0);

        }
        printUsage();
        System.exit(1);
    }
    
    private static Inventory createInventory(StreamFactory streamFactory, String dbDIR) {
        //return new SimpleInventory(streamFactory, new File(dbDIR, "geo_index").getPath());
        final SafeUpdateInventory inventory = new SafeUpdateInventory(streamFactory, dbDIR);
        inventory.setVerbose(false);
        return inventory;
    }

    private static void dump(Inventory inventory, String csvPath) throws IOException {
        inventory.dump(csvPath);
    }

    private static void update(Inventory inventory, String csvPath) throws IOException {
        inventory.updateIndex(csvPath);
    }

    private static void query(Inventory inventory, String[] args) throws IOException {
        Constrain constraints = parseConstraint(args);
        //long t1 = System.currentTimeMillis();
        List<String> pathList = inventory.query(constraints);
        //long t2 = System.currentTimeMillis();
        //System.err.printf("Time needed: %dms%n", (t2 - t1));
        //System.err.printf("Num results: %d%n", pathList.size());
        for (String path : pathList) {
            System.err.println(path);
        }
    }

    private static Constrain parseConstraint(String[] args) throws IOException {
        Constrain.Builder cb = new Constrain.Builder();
        String start = null;
        String end = null;
        for (int i = 2; i < args.length; ) {
            String key = args[i++];
            String value = args[i++];
            switch (key) {
                case "startTime":
                    start = value;
                    break;
                case "endTime":
                    end = value;
                    break;
                case "wkt":
                    cb.withPolygon(value);
                    break;
                case "insitu":
                    cb.withInsituRecords(InsituRecords.read(new File(value), SimpleRecord.INSITU_DATE_FORMAT));
                    cb.withInsituTimeDelta(HOURS_IN_MILLIS * 3);
                    cb.useOnlyProductStartDate(false);
                    break;
                default:
                    System.out.println("unknown parameters for query: " + key);
                    printUsage();
                    System.exit(1);
            }
        }
        if (start != null || end != null) {
            cb.addDateRang(start, end);
        }
        return cb.build();
    }

    private static void printUsage() {
        System.out.println("geo-inventiory [create|update|query] <DB-dir> ...");
        System.out.println("");
        System.out.println("update <DB-dir> <CSV-path>");
        System.out.println("    updates the DB from the given CSV file");
        System.out.println("dump <DB-dir> <CSV-path>");
        System.out.println("    writes the content of the DB to the CSV file");
        System.out.println("query <DB-dir> <constraints>");
        System.out.println("     queries the DB using the given constraints:");
        System.out.println("     startTime YYYY-MM-DD");
        System.out.println("     endTime YYYY-MM-DD");
        System.out.println("     wkt POLYGON((...))");
        System.out.println("     insitu MATCHUP_FILE");
    }
}
