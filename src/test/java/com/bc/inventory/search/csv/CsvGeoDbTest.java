package com.bc.inventory.search.csv;

import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.GeoDbEntry;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class CsvGeoDbTest {

    private static final String NORTHSEA_WKT = "polygon((-19.94 40.00, -20.00 60.00, 0.0 60.00, 0.00 65.00, 13.06 65.00, 12.99 53.99, 0.00 49.22,  0.00 40.00,  -19.94 40.00))";

    private ImageInputStream iis;
    private CsvGeoDb csvGeoDb;
    private Constrain northsea;
    private Constrain later;

    @Before
    public void setUp() throws Exception {
        InputStream is = this.getClass().getResourceAsStream("/meris20050101_products_list.csv");
        iis = new MemoryCacheImageInputStream(is);
        csvGeoDb = new CsvGeoDb();
        northsea = new Constrain.Builder("northsea").polygon(NORTHSEA_WKT).build();
        later = new Constrain.Builder("later").startDate("2005-05-05").build();
    }

    @Test
    public void testOpen() throws Exception {
        csvGeoDb.open(iis);
        Iterator<GeoDbEntry> entryIterator = csvGeoDb.entries();
        assertNotNull(entryIterator);
        ArrayList<GeoDbEntry> entryList = Lists.newArrayList(entryIterator);
        assertEquals(14, entryList.size());
    }

    @Test
    public void testQuery() throws Exception {
        csvGeoDb.open(iis);
        List<String> result = csvGeoDb.query(northsea);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("MER_RR__1PNUPA20050101_105248_000026182033_00266_14849_3950.N1", result.get(0));
        assertEquals("MER_RR__1PNUPA20050101_123323_000026182033_00267_14850_3951.N1", result.get(1));
        
        result = csvGeoDb.query(later);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdater()throws Exception {
        csvGeoDb.open(iis);
        csvGeoDb.getDbUpdater();
        fail();
    }
}