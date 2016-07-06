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
 * A header is used to describe the {@link Record}s provided by a {@link RecordSource}.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface Header {

    /**
     * @return {@code true}, if records that conform to this header return location values (see {@link Record#getLocation()}).
     */
    boolean hasLocation();

    /**
     * @return {@code true}, if records that conform to this header return time values (see {@link Record#getTime()}).
     */
    boolean hasTime();

    /**
     * @return The array of attribute names.
     */
    String[] getAttributeNames();

    /**
     * @return The index of the attribute.
     */
    int getAttributeIndex(String name);

    /**
     * @param index The column index of the attribute, starting from column 0
     *
     * @return The attribute of the respective column, or null if index is less than 0
     */
    String getAttributeName(int index);

    /**
     * @return The array of annotation names.
     */
    String[] getAnnotationNames();

    /**
     * @return The index of the annotation.
     */
    int getAnnotationIndex(String name);
}

