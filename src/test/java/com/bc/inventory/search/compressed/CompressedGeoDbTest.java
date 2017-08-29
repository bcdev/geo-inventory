package com.bc.inventory.search.compressed;

import com.bc.geometry.s2.S2WKTReader;
import com.bc.inventory.search.Constrain;
import com.bc.inventory.search.GeoDbEntry;
import com.bc.inventory.search.GeoDbUpdater;
import com.bc.inventory.utils.TimeUtils;
import com.google.common.collect.Lists;
import com.google.common.geometry.S2Polygon;
import org.junit.Before;
import org.junit.Test;

import javax.imageio.stream.MemoryCacheImageInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class CompressedGeoDbTest {

    private static final String A_MODIS_WKT = "POLYGON((-96.5327666830256 87.1546434730307,103.743686712102 71.3437478705495,59.2615258995161 65.0182420500955,0.362928123129433 73.3758030120494,-96.5327666830256 87.1546434730307))";
    private static final String B_MODIS_WKT = "POLYGON((151.367472095849 60.7275105379226,111.850519483775 55.1885549599343,84.5828043649338 67.688734896781,155.300624162535 78.4407612780669,151.367472095849 60.7275105379226))";
    private static final DateFormat DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd");
                         
    private S2Polygon aModisPolygon;
    private S2Polygon bModisPolygon;

    @Before
    public void setUp() throws Exception {
        S2WKTReader wktReader = new S2WKTReader();
        aModisPolygon = (S2Polygon) wktReader.read(A_MODIS_WKT);
        bModisPolygon = (S2Polygon) wktReader.read(B_MODIS_WKT);
    }

    @Test
    public void testCreateAndQuery() throws Exception {
        CompressedGeoDb compressedGeoDb = new CompressedGeoDb();
        GeoDbUpdater dbUpdater = compressedGeoDb.getDbUpdater();
        dbUpdater.addEntry(new GeoDbEntry(startAsInt("2005-01-01"), endAsInt("2005-01-05"), "p1", aModisPolygon));
        dbUpdater.addEntry(new GeoDbEntry(startAsInt("2005-01-07"), endAsInt("2005-01-10"), "p2", bModisPolygon));
        
        ArrayList<GeoDbEntry> entryList = Lists.newArrayList(compressedGeoDb.entries());
        assertEquals(2, entryList.size());
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dbUpdater.write(baos);
        assertEquals(343, baos.size());
        
        MemoryCacheImageInputStream iis = new MemoryCacheImageInputStream(new ByteArrayInputStream(baos.toByteArray()));
        CompressedGeoDb compressedGeoDb2 = new CompressedGeoDb();
        compressedGeoDb2.open(iis);
        ArrayList<GeoDbEntry> entryList2 = Lists.newArrayList(compressedGeoDb2.entries());
        assertEquals(2, entryList2.size());

        Constrain constrain = new Constrain.Builder("q").startDate("2005-01-06").build();
        List<String> result = compressedGeoDb2.query(constrain);
        assertEquals(1, result.size());
    }
    
    private int startAsInt(String date) throws ParseException {
        return TimeUtils.startTimeInMin(DATE_FORMAT.parse(date).getTime());      
    }
    
    private int endAsInt(String date) throws ParseException {
        return TimeUtils.endTimeInMin(DATE_FORMAT.parse(date).getTime());      
    }
    
}