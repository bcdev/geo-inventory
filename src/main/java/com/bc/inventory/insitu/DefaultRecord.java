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
import java.util.Arrays;
import java.util.Date;

/**
 * A default implementation of a {@link Record}.
 *
 * @author MarcoZ
 * @author Norman
 */
public class DefaultRecord implements Record {

    private final int id;
    private final Point2D.Float location;
    private final Date time;
    private final Object[] attributeValues;
    private final Object[] annotationValues;

    public DefaultRecord(int id, Point2D.Float location, Date time, Object[] attributeValues) {
        this(id, location, time, attributeValues, new Object[]{""});
    }

    public DefaultRecord(int id, Point2D.Float location, Date time, Object[] attributeValues, Object[] annotationValues) {
        this.id = id;
        this.location = location;
        this.time = time;
        this.attributeValues = attributeValues;
        this.annotationValues = annotationValues;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Point2D.Float getLocation() {
        return location;
    }

    @Override
    public Date getTime() {
        return time;
    }

    @Override
    public Object[] getAttributeValues() {
        return attributeValues;
    }

    @Override
    public Object[] getAnnotationValues() {
        return annotationValues;
    }

    @Override
    public String toString() {
        return "DefaultRecord{" +
               "id=" + id +
               ", location=" + location +
               ", time=" + time +
               ", values=" + Arrays.asList(attributeValues) +
               '}';
    }
}
