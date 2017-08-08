package com.bc.inventory.search;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Created by marco on 26.02.17.
 */
public class UsageExample {


    public void main(String[] args) throws IOException {
//        String dir = "DB_DIR";
//        StreamFactory io = new FileStreamFactory(new File(dir));
//
//        // writing scans
//        String scanId = "uniqeu_ID";
//        OutputStream os = io.writeScan(scanId);
//
//        IndexLayout indexLayout = new RobustLayout(io);
//        // query
//        Inventory inv1 = new BitmapInventory(io);
//        inv1.load();
//        {
//            String[] dbs = io.listByTime("geo_db.*");
//            db = io.open(dbs[0]);
//            String[] scanFiles = io.list("scan.*");
//            for (String scanFile : scanFiles) {
//                io.open(scanFile);
//            }
//        }
//        Constrain constrain = new Constrain.Builder().build(); // dummy
//        inv1.query(constrain);
//
//        // rebuilding
//        Inventory inv2 = new BitmapInventory(io);
//        inv2.rebuild();
//        {
//            String[] dbs = io.listByTime("geo_db.*");
//            db = io.open(dbs[0]);
//            String[] scanFiles = io.list("scan.*");
//            for (String scanFile : scanFiles) {
//                io.open(scanFile);
//            }
//            OutputStream newIndexOs = io.create("tmp");
//            // write to newIndexOs
//            // test  newIndexOs (size)
//            io.delete(dbs[dbs.length-1]);
//            io.rename("tmp", dbs[dbs.length-1]);
//            for (String scanFile : scanFiles) {
//                io.move(scanFile, "archive/"+ scanFile);
//            }
//        }
//         // adding to index
//        CsvRecordReader.CsvRecordIterator csvRecordIterator = null;
//        Inventory inv3 = new BitmapInventory(io);
//        inv3.add(csvRecordIterator);
//
//
//        IndexLayout indexLayout2 = new RobustLayout(io);

    }
    /**
     * Interface for access to different index layouts.
     */
    public interface IndexLayout {
    }

    private class CombinedInventory {
        private final String dir;
        private final Inv zInventory;
        private final Inv csvInventory;

        public CombinedInventory(String dir) {
            this.dir = dir;
            zInventory = new ZInventory();
            csvInventory = new ZInventory();
        }
        
        private void loadInventories() {
            
            // list ".a" and ".b" sort by age
            
            InputStream zis = null; // open newer one, if any
            zInventory.read(zis);
            
            List<String> csvFiles = null; // TODO list ALL CSV
            for (String csvFile : csvFiles) {
                InputStream is = null; // TODO from csvFile
                csvInventory.read(is);
            }
            
        }

        public void updateIndex() {
            loadInventories();

            Iterator<Entry> entries = csvInventory.entries();
            while (entries.hasNext()) {
                zInventory.addEntry(entries.next());
            }
            
            OutputStream os = null; // TODO create for ".new", print warnig if it exist
            zInventory.write(os);
            
            // remove older one from ".a" and ".b"
            // rename ".new" to older name
            
            // concat csvFiles to attic/<TIMESTAMP>
            
        }
        
        public List<String> query(Constrain constrain) {
            loadInventories();
            
            List<String> result1 = zInventory.query(constrain);
            List<String> result2 = zInventory.query(constrain);
            
            List<String> mergedResult = null;
            //merge result1 and result2
            return mergedResult;
        }

    }

    private class Inv {

        public void read(InputStream zis) {
            
        }

        public Iterator<Entry> entries() {
            return null;
        }

        public void addEntry(Entry next) {
            
        }

        public void write(OutputStream os) {
            
        }
        
        public List<String> query(Constrain constrain) {
            return null;
        }
    }

    private class ZInventory extends Inv {
        public ZInventory() {
        }
    }
    private class Csv2Inventory extends Inv{
        public Csv2Inventory() {
        }
    }
    private class Entry {
    }
}
