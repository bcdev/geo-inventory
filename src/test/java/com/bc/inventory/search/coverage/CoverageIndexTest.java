package com.bc.inventory.search.coverage;

import org.junit.Test;

import static org.junit.Assert.*;

public class CoverageIndexTest {

    @Test
    public void indexedBinarySearch() throws Exception {
        CoverageIndex.IndexRecord[] records = new CoverageIndex.IndexRecord[3];
        records[0] = new CoverageIndex.IndexRecord(3, 6, 0, 0, 0);
        records[1] = new CoverageIndex.IndexRecord(10, 15, 0, 0, 0);
        records[2] = new CoverageIndex.IndexRecord(20, 30, 0, 0, 0);

        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 1));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 2));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 3));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 4));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 5));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 6));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 7));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 8));
        assertEquals(0, CoverageIndex.indexedBinarySearch(records, 9));
        assertEquals(1, CoverageIndex.indexedBinarySearch(records, 10));
        assertEquals(1, CoverageIndex.indexedBinarySearch(records, 11));
    }

}