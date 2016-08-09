package com.bc.inventory.search.coverage;

import org.junit.Test;

import static org.junit.Assert.*;

public class CoverageInventoryTest {
    @Test
    public void indexedBinarySearch() throws Exception {
        int[] values = {3, 10, 20};
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 1));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 2));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 3));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 4));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 5));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 6));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 7));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 8));
        assertEquals(0, CoverageInventory.indexedBinarySearch(values, 9));
        assertEquals(1, CoverageInventory.indexedBinarySearch(values, 10));
        assertEquals(1, CoverageInventory.indexedBinarySearch(values, 11));
    }

}