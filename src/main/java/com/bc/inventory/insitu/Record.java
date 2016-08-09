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

import java.awt.geom.Point2D;
import java.util.Date;

/**
 * A record comprises a coordinate and an array of attribute values for each attribute described in the {@link Header}.
 *
 * @author Norman
 */
public interface Record {

    /**
     * @return The id of this record,
     *         must be unique within a single {@link RecordSource}.
     */
    int getId();

    /**
     * @return The location as (lat,lon) point or {@code null} if the location is not available (see {@link Header#hasLocation()}).
     *         The location is usually represented in form of one or more attribute values.
     *         This is the location of the corresponding reference record.
     */
    Point2D getLocation();

    /**
     * @return The UTC time in milliseconds or {@code null} if the time is not available (see {@link Header#hasTime()}).
     *         This is the time of the corresponding reference record.
     */
    Date getTime();

    /**
     * @return The attribute values according to {@link Header#getAttributeNames()}.
     *         The array will be empty if this record doesn't have any attributes.
     */
    Object[] getAttributeValues();

    /**
     * @return The annotation values according to {@link Header#getAnnotationNames()}.
     *         The array will be empty if this record doesn't have any annotations.
     */
    Object[] getAnnotationValues();

}
