package com.bc.inventory.search.ng;

import org.junit.Test;

import static org.junit.Assert.*;

public class NgInventoryTest {
    @Test
    public void indexedBinarySearch() throws Exception {
        int[] values = {3, 10, 20};
        assertEquals(0, NgInventory.indexedBinarySearch(values, 1));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 2));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 3));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 4));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 5));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 6));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 7));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 8));
        assertEquals(0, NgInventory.indexedBinarySearch(values, 9));
        assertEquals(1, NgInventory.indexedBinarySearch(values, 10));
        assertEquals(1, NgInventory.indexedBinarySearch(values, 11));
    }

}