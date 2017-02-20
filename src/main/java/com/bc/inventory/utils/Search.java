package com.bc.inventory.utils;

/**
 * @author marcoz
 */
public class Search {

    public static int indexedBinarySearch(int[] startTimes, int currentStartTime) {
        int low = 0;
        int high = startTimes.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            final int t1 = startTimes[mid];
            if (t1 < currentStartTime) {
                low = mid + 1;
            } else if (t1 == currentStartTime) {
                return mid; // key found
            } else {
                high = mid - 1;
            }
        }
        return low == 0 ? low : low - 1;  // key not found
    }
}
