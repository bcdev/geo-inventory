/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.inventory.utils;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLng;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Range.greaterThan;

/**
 * S2 methods dealing with a resolution that fits into an integer (32 bit)
 */
public class S2Integer {

    public static int asInt(S2CellId s2CellId) {
        if (s2CellId.level() > 13) {
            s2CellId = s2CellId.parent(13);
        }
        return (int) (s2CellId.id() >>> 34);
    }

    public static strictfp boolean containsCellId(final int[] intCellIds, final S2CellId s2CellId) {
        return containsCellId(intCellIds, asInt(s2CellId));
    }

    public static strictfp boolean containsCellId(final int[] intCellIds, final int s2CellIdInt) {
        if (rangeMin(intCellIds[0]) > s2CellIdInt) {
            return false;
        }
        if (rangeMax(intCellIds[intCellIds.length - 1]) < s2CellIdInt) {
            return false;
        }
        int pos = Arrays.binarySearch(intCellIds, s2CellIdInt);
        if (pos < 0) {
            pos = -pos - 1;
        }

        return pos < intCellIds.length && rangeMin(intCellIds[pos]) <= s2CellIdInt || pos != 0 && rangeMax(intCellIds[pos - 1]) >= s2CellIdInt;
    }

    public static strictfp boolean intersectsCellId(final int[] intCellIds, final int s2CellIdInt) {
        int pos = Arrays.binarySearch(intCellIds, s2CellIdInt);
        if (pos < 0) {
            pos = -pos - 1;
        }

        return pos < intCellIds.length && rangeMin(intCellIds[pos]) <= rangeMax(s2CellIdInt) || pos != 0 && rangeMax(intCellIds[pos - 1]) >= rangeMin(s2CellIdInt);
    }

    public static int rangeMin(int s2cell) {
        return s2cell - (lowestOnBit(s2cell) - 1);
    }

    public static int rangeMax(int s2cell) {
        return s2cell + (lowestOnBit(s2cell) - 1);
    }

    public static int lowestOnBit(int s2cell) {
        return s2cell & -s2cell;
      }

    public static int[] convertCellUnion(S2CellUnion cellUnion) {
        int[] intIds;ArrayList<S2CellId> s2CellIds = cellUnion.cellIds();
        intIds = new int[s2CellIds.size()];
        for (int i = 0; i < intIds.length; i++) {
            intIds[i] = S2Integer.asInt(s2CellIds.get(i));
        }
        return intIds;
    }

    public static boolean intersectsCellUnion(int[] c1, int[] c2) {
        for (int s2CellIdInt : c2) {
            if (S2Integer.intersectsCellId(c1, s2CellIdInt)) {
                return true;
            }
        }
        return false;
    }

    public static boolean intersectsCellUnionFast(int[] c1, int[] c2) {
        int i = 0;
        int j = 0;

        while (i < c1.length && j < c2.length) {
            int imin = rangeMin(c1[i]);
            int jmin = rangeMin(c2[j]);
            if (imin > jmin) {
                // Either j->contains(*i) or the two cells are disjoint.
                if (c1[i] <= rangeMax(c2[j])) {
                    return true;
                } else {
                    // Advance "j" to the first cell possibly contained by *i.
                    j = indexedBinarySearch(c2, imin, j + 1);
                    // The previous cell *(j-1) may now contain *i.
                    if (c1[i] <= rangeMax(c2[j - 1])) {
                        --j;
                    }
                }
            } else if (jmin > imin) {
                // Identical to the code above with "i" and "j" reversed.
                if (c2[j] <= rangeMax(c1[i])) {
                    return true;
                } else {
                    i = indexedBinarySearch(c1, jmin, i + 1);
                    if (c2[j] <= rangeMax(c1[i - 1])) {
                        --i;
                    }
                }
            } else {
                // "i" and "j" have the same range_min(), so one contains the other.
                return true;
            }
        }
        return false;
    }
    /**
     * Just as normal binary search, except that it allows specifying the starting
     * value for the lower bound.
     *
     * @return The position of the searched element in the list (if found), or the
     *         position where the element could be inserted without violating the
     *         order.
     */
    private static int indexedBinarySearch(int[] l, int key, int low) {
      int high = l.length - 1;

      while (low <= high) {
        int mid = (low + high) >> 1;
        int midVal = l[mid];

        if (midVal < key) {
          low = mid + 1;
        } else if (midVal > key) {
          high = mid - 1;
        } else {
          return mid; // key found
        }
      }
      return low; // key not found
    }



    public static class Coverage {

        public final int[] intIds;

        public Coverage(S2CellUnion cellUnion) {
            intIds = convertCellUnion(cellUnion);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Coverage)) return false;
            return Arrays.equals(intIds, ((Coverage) other).intIds);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(intIds);
        }
    }


    public static void main(String[] args) {
        S2LatLng s2LatLng = S2LatLng.fromDegrees(42, 10);
        System.out.println("s2LatLng = " + s2LatLng);
        S2CellId s2CellId = S2CellId.fromLatLng(s2LatLng);
        System.out.println("s2CellId = " + s2CellId);
        S2CellId s2CellId13 = s2CellId.parent(13);
        System.out.println("s2CellId13 = " + s2CellId13);

        System.out.println("s2cellId = " + Long.toBinaryString(s2CellId.id()));
        System.out.println("s2cellId13 = " + Long.toBinaryString(s2CellId13.id()));
        System.out.println("s2cellIdInt = " + Long.toBinaryString(asInt(s2CellId)));
    }
}
