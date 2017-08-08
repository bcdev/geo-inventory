package com.bc.inventory.utils;

/**
 * @author MarcoZ
 */
public class Search {

    /**
        * Searches the specified array of ints for the specified value using the
        * binary search algorithm.  The array must be sorted (as
        * by the {@link java.util.Arrays#sort(int[])} method) prior to making this call.  If it
        * is not sorted, the results are undefined.  If the array contains
        * multiple elements with the specified value, the first values is returned.
        *
        * @param array the array to be searched
        * @param key the value to be searched for
        * @return index of the search key, if it is contained in the array;
        *         otherwise, <tt><i>insertion point</i> - 1</tt>.  The
        *         <i>insertion point</i> is defined as the point at which the
        *         key would be inserted into the array: the index of the first
        *         element greater than the key, or <tt>a.length</tt> if all
        *         elements in the array are less than the specified key.
        */
    public static int indexedBinarySearch(int[] array, int key) {
        int low = 0;
        int high = array.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            final int t1 = array[mid];
            if (t1 < key) {
                low = mid + 1;
            } else if (t1 == key) {
                // if there multiple equal values next to each other, 
                // make sure to return the index to first of them
                while (mid > 0 && array[mid - 1] == key) {
                    mid--;
                }
                return mid; // key found
            } else {
                high = mid - 1;
            }
        }
        return low == 0 ? low : low - 1;  // key not found
    }
}
