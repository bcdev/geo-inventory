package com.bc.inventory;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.FileStreamFactory;
import com.bc.inventory.search.QueryResult;
import com.bc.inventory.search.StreamFactory;
import com.bc.inventory.search.ng.NgInventory;

import java.io.File;
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

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            printUsage();
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
        NgInventory inventory = new NgInventory(streamFactory);
        inventory.createIndex(csvFile);
    }

    private static void dump(StreamFactory streamFactory, String csvFile) throws IOException {
        NgInventory inventory = new NgInventory(streamFactory);
        inventory.loadIndex();
        inventory.writeDB(csvFile);
    }

    private static void update(StreamFactory streamFactory, String csvFile) throws IOException {
        NgInventory inventory = new NgInventory(streamFactory);
        inventory.updateIndex(csvFile);
    }

    private static void query(StreamFactory streamFactory, String[] args) throws IOException {
        NgInventory inventory = new NgInventory(streamFactory);
        inventory.loadIndex();
        Constrain constraints = parseConstraint(args);
        QueryResult queryResult = inventory.query(constraints);
        Collection<String> paths = queryResult.getPaths();
        System.out.println("Query result:");
        System.out.println();
        for (String path : paths) {
            System.out.println(path);
        }
        System.out.println();
    }

    private static Constrain parseConstraint(String[] args) {
        Constrain constrain = new Constrain();
        for (int i = 2; i < args.length; ) {
            String key = args[i++];
            String value = args[i++];
            switch (key) {
                case "startTime":
                    constrain.withStartDate(value);
                    break;
                case "endTime":
                    constrain.withEndDate(value);
                    break;
                case "wkt":
                    constrain.withPolygon(value);
                    break;
                default:
                    System.out.println("unknown parameters for query: " + key);
                    printUsage();
                    System.exit(1);
            }
        }
        return constrain;
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
        System.out.println("     startTime YY-MM-DD.....");
    }
}
