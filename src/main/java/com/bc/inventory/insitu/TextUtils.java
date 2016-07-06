/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.inventory.insitu;

/**
 * Various text utility functions.
 *
 * @author Norman
 */
public class TextUtils {

    public static int indexOf(String[] textValues, String possibleValue) {
        return indexOf(textValues, new String[] {possibleValue});
    }

    public static int indexOf(String[] textValues, String[] possibleValues) {
        for (String possibleValue : possibleValues) {
            for (int index = 0; index < textValues.length; index++) {
                if (possibleValue.equalsIgnoreCase(textValues[index])) {
                    return index;
                }
            }
        }
        return -1;
    }

    public static String join(Object[] tokens, String separator) {

        if (tokens == null) {
            throw new IllegalArgumentException();
        }

        if (separator == null) {
            throw new IllegalArgumentException();
        }

        StringBuilder sb = new StringBuilder(tokens.length * 16);
        for (int i = 0; i < tokens.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            if (tokens[i] != null) {
                sb.append(tokens[i].toString());
            }
        }

        return sb.toString();
    }
}
