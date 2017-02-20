package com.bc.inventory;

import com.bc.inventory.insitu.InsituRecords;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.FileStreamFactory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.bitmap.BitmapInventory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

/**
 * A command line interface to the geo inventory.
 * The following options are supported:
 * <p>
 * create <DB-dir> <CSV-file>
 *   creates an DB from the given CSV file
 * <p>
 * update <DB-dir> <CSV-file>
 *   updates the DB from the given CSV file
 * <p>
 * dump <DB-dir> <CSV-file>
 *   writes the content of the DB to the CSV file
 * <p>
 * query <DB-dir> <constraints>
 *   queries the DB using the given constraints.
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
        StreamFactory streamFactory = new FileStreamFactory(new File(dbDIR));
        switch (mode) {
            case "create":
                create(streamFactory, args[2]);
                System.exit(0);
            case "update":
                update(streamFactory, args[2]);
                System.exit(0);
            case "query":
                query(streamFactory, args);
                System.exit(0);
            case "dump":
                dump(streamFactory, args[2]);
                System.exit(0);

        }
        printUsage();
        System.exit(1);
    }

    private static void create(StreamFactory streamFactory, String csvFile) throws IOException {
        BitmapInventory inventory = new BitmapInventory(streamFactory);
        inventory.createIndex(csvFile);
    }

    private static void dump(StreamFactory streamFactory, String csvFile) throws IOException {
        BitmapInventory inventory = new BitmapInventory(streamFactory);
        inventory.loadIndex();
        inventory.dumpDB(csvFile);
    }

    private static void update(StreamFactory streamFactory, String csvFile) throws IOException {
        BitmapInventory inventory = new BitmapInventory(streamFactory);
        inventory.updateIndex(csvFile);
    }

    private static void query(StreamFactory streamFactory, String[] args) throws IOException {
        BitmapInventory inventory = new BitmapInventory(streamFactory);
        inventory.loadIndex();
        System.out.println("numEntries in geoDB= " + inventory.numEntries());
        Constrain constraints = parseConstraint(args);
        System.out.println("Constraints = " + constraints);
        long t1 = System.currentTimeMillis();
        QueryResult queryResult = inventory.query(constraints);
        long t2 = System.currentTimeMillis();
        Collection<String> paths = queryResult.getPaths();
        final String outputFilename = "geoDB_query_result.txt";
        System.out.printf("Time needed: %dms%n", (t2-t1));
        System.out.printf("Num results: %d%n", paths.size());
        try(FileWriter fw = new FileWriter(outputFilename)) {
            for (String path : paths) {
                fw.append(path).append('\n');
            }
        }
        System.out.println();
        System.out.printf("Query result written to file: '%s'%n", outputFilename);
    }

    private static Constrain parseConstraint(String[] args) throws IOException {
        Constrain.Builder cb = new Constrain.Builder();
        for (int i = 2; i < args.length; ) {
            String key = args[i++];
            String value = args[i++];
            switch (key) {
                case "startTime":
                    cb.startDate(value);
                    break;
                case "endTime":
                    cb.endDate(value);
                    break;
                case "wkt":
                    cb.polygon(value);
                    break;
                case "insitu":
                    cb.insitu(InsituRecords.read(new File(value)));
                    cb.timeDelta(HOURS_IN_MILLIS * 3);
                    cb.useOnlyProductStartDate(false);
                    break;
                default:
                    System.out.println("unknown parameters for query: " + key);
                    printUsage();
                    System.exit(1);
            }
        }
        return cb.build();
    }

    private static void printUsage() {
        System.out.println("geo-inventiory [create|update|query] <DB-dir> ...");
        System.out.println("");
        System.out.println("create <DB-dir> <CSV-file>");
        System.out.println("     creates an DB from the given CSV file");
        System.out.println("update <DB-dir> <CSV-file>");
        System.out.println("    updates the DB from the given CSV file");
        System.out.println("dump <DB-dir> <CSV-file>");
        System.out.println("    writes the content of the DB to the CSV file");
        System.out.println("query <DB-dir> <constraints>");
        System.out.println("     queries the DB using the given constraints:");
        System.out.println("     startTime YYYY-MM-DD");
        System.out.println("     endTime YYYY-MM-DD");
        System.out.println("     wkt POLYGON((...))");
        System.out.println("     insitu MATCHUP_FILE");
    }
}
