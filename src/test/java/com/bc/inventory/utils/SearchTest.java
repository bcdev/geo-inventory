package com.bc.inventory.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SearchTest {
    @Test
    public void search() throws Exception {
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
        assertEquals(2, Search.indexedBinarySearch(values, 20));
        assertEquals(2, Search.indexedBinarySearch(values, 21));
    }
    
    @Test
    public void oneValue() throws Exception {
        int[] values = {2};
        assertEquals(0, Search.indexedBinarySearch(values, 1));
        assertEquals(0, Search.indexedBinarySearch(values, 2));
        assertEquals(0, Search.indexedBinarySearch(values, 3));
    }

    @Test
    public void equalValues() throws Exception {
        int[] values = {3, 4, 4};
        assertEquals(0, Search.indexedBinarySearch(values, 1));
        assertEquals(0, Search.indexedBinarySearch(values, 2));
        assertEquals(0, Search.indexedBinarySearch(values, 3));
        assertEquals(1, Search.indexedBinarySearch(values, 4));
        assertEquals(2, Search.indexedBinarySearch(values, 5));
        
        values = new int[]{4, 4, 4};
        assertEquals(0, Search.indexedBinarySearch(values, 1));
        assertEquals(0, Search.indexedBinarySearch(values, 2));
        assertEquals(0, Search.indexedBinarySearch(values, 3));
        assertEquals(0, Search.indexedBinarySearch(values, 4));
        assertEquals(2, Search.indexedBinarySearch(values, 5));

        values = new int[]{4, 4, 4, 4, 4};
        assertEquals(0, Search.indexedBinarySearch(values, 1));
        assertEquals(0, Search.indexedBinarySearch(values, 2));
        assertEquals(0, Search.indexedBinarySearch(values, 3));
        assertEquals(0, Search.indexedBinarySearch(values, 4));
        assertEquals(4, Search.indexedBinarySearch(values, 5));
        
    }
    
}