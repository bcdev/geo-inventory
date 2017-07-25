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

package com.bc.inventory.utils;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;

import java.awt.geom.Point2D;
import java.text.DateFormat;
import java.util.Date;

public class SimpleRecord {

    public static final DateFormat INSITU_DATE_FORMAT = TimeUtils.createDateFormat("yyyy-MM-dd HH:mm:ss");

    private final long time;
    private final Point2D location;

    public SimpleRecord(Point2D location) {
        this(-1L, location);
    }

    public SimpleRecord(long time, Point2D location) {
        this.time = time;
        this.location = location;
    }

    public long getTime() {
        return time;
    }

    public Point2D getLocation() {
        return location;
    }

    public S2Point getAsPoint() {
        final double lon = location.getX();
        final double lat = location.getY();
        S2LatLng s2LatLng = S2LatLng.fromDegrees(lat, lon);
        return s2LatLng.toPoint();
    }

    @Override
    public String toString() {
        return "SimpleRecord{" +
                "time=" + INSITU_DATE_FORMAT.format(new Date(time)) +
                ", location=" + location +
                '}';
    }
}
