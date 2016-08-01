package com.bc.inventory.search.ng;

import com.bc.geometry.s2.S2WKTReader;
import com.google.common.geometry.S2Polygon;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.*;

public class IndexCreatorTest {


    private static final S2Polygon aModisPolygon;
    private static final S2Polygon bModisPolygon;

    static {
        String A_MODIS_WKT = "POLYGON((-96.5327666830256 87.1546434730307,103.743686712102 71.3437478705495,59.2615258995161 65.0182420500955,0.362928123129433 73.3758030120494,-96.5327666830256 87.1546434730307))";
        String B_MODIS_WKT = "POLYGON((151.367472095849 60.7275105379226,111.850519483775 55.1885549599343,84.5828043649338 67.688734896781,155.300624162535 78.4407612780669,151.367472095849 60.7275105379226))";
        S2WKTReader wktReader = new S2WKTReader();
        aModisPolygon = (S2Polygon) wktReader.read(A_MODIS_WKT);
        bModisPolygon = (S2Polygon) wktReader.read(B_MODIS_WKT);
    }

    @Test
    public void testIndex() throws Exception {
        IndexCreator indexCreator = new IndexCreator(3);
        indexCreator.addToIndex("p1", 2 * 60 * 1000, 4 * 60 * 1000, aModisPolygon);
        indexCreator.addToIndex("p2", 6 * 60 * 1000, 8 * 60 * 1000, aModisPolygon);
        assertEquals(2, indexCreator.size());

        ByteArrayOutputStream indexOS = new ByteArrayOutputStream();
        ByteArrayOutputStream dataOS = new ByteArrayOutputStream();
        indexCreator.write(indexOS, dataOS);

        assertEquals(96, indexOS.size());
        assertEquals(282, dataOS.size());

        IndexCreator indexCreator2 = new IndexCreator(3);
        assertEquals(0, indexCreator2.size());
        ByteArrayInputStream indexIS = new ByteArrayInputStream(indexOS.toByteArray());
        ByteArrayInputStream dataIS = new ByteArrayInputStream(dataOS.toByteArray());
        indexCreator2.loadExistingIndex(indexIS, dataIS);
        assertEquals(2, indexCreator2.size());

        indexCreator2.removeFromIndex("p1");
        assertEquals(1, indexCreator2.size());
    }
}