package com.bc.inventory.search;

import com.bc.inventory.search.csv.CsvRecordReaderTest;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

public class SafeUpdateFacadeTest {

    @Test
    public void test_empty() throws Exception {
        FileStreamFactory fileStreamFactory = new FileStreamFactory();
        Path tmpDir = Files.createTempDirectory("SafeUpdateFacadeTest");
        SafeUpdateFacade facade = new SafeUpdateFacade(fileStreamFactory, tmpDir.toString());
        Path dumpCSV = tmpDir.resolve("dump1");
        facade.dump(dumpCSV.toString());
        assertTrue(Files.exists(dumpCSV));
        assertEquals(0, Files.size(dumpCSV));
    }
    
    
    @Test
    public void test_create() throws Exception {
        FileStreamFactory fileStreamFactory = new FileStreamFactory();
        Path tmpDir = Files.createTempDirectory("SafeUpdateFacadeTest");
        SafeUpdateFacade facade = new SafeUpdateFacade(fileStreamFactory, tmpDir.toString());
        Path csv1 = copyResource(tmpDir, "/meris20050101_products_list.csv", "CSV_1");

        Path geoIndexA = tmpDir.resolve("geo_index.a");
        Path geoIndexB = tmpDir.resolve("geo_index.b");
        Path geoIndexNew = tmpDir.resolve("geo_index.new");
        
        int i = facade.updateIndex(csv1.toString());
        assertEquals(14, i);

        assertTrue(Files.exists(geoIndexA));
        assertFalse(Files.exists(geoIndexB));
        assertFalse(Files.exists(geoIndexNew));
        assertEquals(39995, Files.size(geoIndexA));
    }
    
    @Test
    public void test_updates() throws Exception {
        FileStreamFactory fileStreamFactory = new FileStreamFactory();
        Path tmpDir = Files.createTempDirectory("SafeUpdateFacadeTest");
        SafeUpdateFacade facade = new SafeUpdateFacade(fileStreamFactory, tmpDir.toString());
        Path csv1 = copyResource(tmpDir, "/meris20050101_products_list.csv", "meris20050101");
        Path csv2 = copyResource(tmpDir, "/meris20050102_products_list.csv", "meris20050102");
        Path csv3 = copyResource(tmpDir, "/meris20050103_products_list.csv", "meris20050103");

        Path geoIndexA = tmpDir.resolve("geo_index.a");
        Path geoIndexB = tmpDir.resolve("geo_index.b");
        Path geoIndexNew = tmpDir.resolve("geo_index.new");
        
        // 1
        int count1 = facade.updateIndex(csv1.toString());
        assertEquals(14, count1);

        assertTrue(Files.exists(geoIndexA));
        assertFalse(Files.exists(geoIndexB));
        assertFalse(Files.exists(geoIndexNew));
        assertEquals(39995, Files.size(geoIndexA));
        assertEquals(1, Files.list(tmpDir.resolve("attic")).count());
        Thread.sleep(1000); // to make sure time stamps are different
        
        // 2
        int count2 = facade.updateIndex(csv2.toString());
        assertEquals(16, count2);

        assertTrue(Files.exists(geoIndexA));
        assertTrue(Files.exists(geoIndexB));
        assertFalse(Files.exists(geoIndexNew));
        assertEquals(39995, Files.size(geoIndexA));
        assertEquals(82315, Files.size(geoIndexB));
        assertEquals(2, Files.list(tmpDir.resolve("attic")).count());
        Thread.sleep(1000); // to make sure time stamps are different
        
        // 3
        int count3 = facade.updateIndex(csv3.toString());
        assertEquals(15, count3);

        assertTrue(Files.exists(geoIndexA));
        assertTrue(Files.exists(geoIndexB));
        assertFalse(Files.exists(geoIndexNew));
        assertEquals(82929, Files.size(geoIndexA));
        assertEquals(82315, Files.size(geoIndexB));
        assertEquals(3, Files.list(tmpDir.resolve("attic")).count());
    }
    
    @Test
    public void test_query() throws Exception {
        FileStreamFactory fileStreamFactory = new FileStreamFactory();
        Path tmpDir = Files.createTempDirectory("SafeUpdateFacadeTest");
        SafeUpdateFacade facade = new SafeUpdateFacade(fileStreamFactory, tmpDir.toString());
        Path csv1 = copyResource(tmpDir, "/meris20050101_products_list.csv", "CSV_20050101");
        Path csv2 = copyResource(tmpDir, "/meris20050102_products_list.csv", "CSV_20050102");
        Path csv3 = copyResource(tmpDir, "/meris20050103_products_list.csv", "CSV_20050103");

        List<String> result = facade.query(new Constrain.Builder("").build());
        assertEquals(45, result.size());
    }
    
    @Test
    public void test_update_then_query() throws Exception {
        FileStreamFactory fileStreamFactory = new FileStreamFactory();
        Path tmpDir = Files.createTempDirectory("SafeUpdateFacadeTest");
        SafeUpdateFacade facade = new SafeUpdateFacade(fileStreamFactory, tmpDir.toString());
        Constrain constrain = new Constrain.Builder("").build();
        
        Path csv1 = copyResource(tmpDir, "/meris20050101_products_list.csv", "CSV_20050101");
        facade.updateIndex(csv1.toString());

        List<String> result = facade.query(constrain);
        assertEquals(14, result.size());
        
        Path csv2 = copyResource(tmpDir, "/meris20050102_products_list.csv", "CSV_20050102");
        Path csv3 = copyResource(tmpDir, "/meris20050103_products_list.csv", "CSV_20050103");
        
        result = facade.query(constrain);
        assertEquals(45, result.size());
        
    }
    
    private Path copyResource(Path destDir, String resourceName, String targetName) throws IOException {
        Path csv1 = destDir.resolve(targetName);
        try (InputStream is = this.getClass().getResourceAsStream(resourceName)) {
            Files.copy(is, csv1);
        }
        return csv1;
    }
}