package com.bc.inventory.search;

import com.bc.inventory.search.csv.CsvGeoDb;
import org.junit.Test;

import javax.imageio.stream.MemoryCacheImageInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.*;

public class QuerySolverTest {

    @Test
    public void test_with_time() throws Exception {
        // testdata_1 contains time information for all products
        CsvGeoDb csvGeoDb = loadDbFromResource("/testdata_1.csv");
        List<String> result;

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-02", "1970-01-02").build());
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("p2", result.get(0));

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-03", "1970-01-03").build());
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("p3a", result.get(0));
        assertEquals("p3b", result.get(1));

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-02", null).build());
        assertNotNull(result);
        assertEquals(4, result.size());
        assertEquals("p2", result.get(0));
        assertEquals("p3a", result.get(1));
        assertEquals("p3b", result.get(2));
        assertEquals("p4", result.get(3));

        result = csvGeoDb.query(new Constrain.Builder().build());
        assertNotNull(result);
        assertEquals(5, result.size());
        assertEquals("p1", result.get(0));
        assertEquals("p2", result.get(1));
        assertEquals("p3a", result.get(2));
        assertEquals("p3b", result.get(3));
        assertEquals("p4", result.get(4));

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-05", "").build());
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void test_without_time() throws Exception {
        // testdata_2 contains no time information for all products
        CsvGeoDb csvGeoDb = loadDbFromResource("/testdata_2.csv");
        List<String> result;

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-02", "1970-01-02").build());
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("p1", result.get(0));
        assertEquals("p2", result.get(1));
        assertEquals("p3", result.get(2));

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-02", "").build());
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("p1", result.get(0));
        assertEquals("p2", result.get(1));
        assertEquals("p3", result.get(2));

        result = csvGeoDb.query(new Constrain.Builder().build());
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("p1", result.get(0));
        assertEquals("p2", result.get(1));
        assertEquals("p3", result.get(2));
    }

    @Test
    public void test_with_and_without_time() throws Exception {
        // testdata_3 contains time information for some products and no time information for other products
        CsvGeoDb csvGeoDb = loadDbFromResource("/testdata_3.csv");
        List<String> result;

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-03", "1970-01-03").build());
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("p0", result.get(0));
        assertEquals("p3", result.get(1));

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("1970-01-04", "").build());
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("p0", result.get(0));

        result = csvGeoDb.query(new Constrain.Builder().addDateRang("", "1970-01-01").build());
        assertNotNull(result);
        System.out.println("result = " + result);
        assertEquals(2, result.size());
        assertEquals("p0", result.get(0));
        assertEquals("p1", result.get(1));
        
        result = csvGeoDb.query(new Constrain.Builder().build());
        assertNotNull(result);
        assertEquals(4, result.size());
        assertEquals("p0", result.get(0));
        assertEquals("p1", result.get(1));
        assertEquals("p2", result.get(2));
        assertEquals("p3", result.get(3));
    }
    
    private CsvGeoDb loadDbFromResource(String name) throws IOException {
        InputStream is = this.getClass().getResourceAsStream(name);
        CsvGeoDb csvGeoDb = new CsvGeoDb();
        csvGeoDb.open(new MemoryCacheImageInputStream(is));
        return csvGeoDb;
    }

}