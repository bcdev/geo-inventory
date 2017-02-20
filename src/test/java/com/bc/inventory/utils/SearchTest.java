package com.bc.inventory.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SearchTest {
    @Test
    public void indexedBinarySearch() throws Exception {
        int[] values = {3, 10, 20};
        assertEquals(0, Search.indexedBinarySearch(values, 1));
        assertEquals(0, Search.indexedBinarySearch(values, 2));
        assertEquals(0, Search.indexedBinarySearch(values, 3));
        assertEquals(0, Search.indexedBinarySearch(values, 4));
        assertEquals(0, Search.indexedBinarySearch(values, 5));
        assertEquals(0, Search.indexedBinarySearch(values, 6));
        assertEquals(0, Search.indexedBinarySearch(values, 7));
        assertEquals(0, Search.indexedBinarySearch(values, 8));
        assertEquals(0, Search.indexedBinarySearch(values, 9));
        assertEquals(1, Search.indexedBinarySearch(values, 10));
        assertEquals(1, Search.indexedBinarySearch(values, 11));
    }
}