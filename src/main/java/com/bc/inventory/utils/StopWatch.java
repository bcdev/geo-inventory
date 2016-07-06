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

public class StopWatch implements AutoCloseable{
    private final String message;
    private final long startTime;

    public StopWatch(String message) {
        this.message = message;
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public void close() {
        long endtime = System.currentTimeMillis();
        long delta = (endtime-startTime);
        float seconds = delta / 1000f;
        System.out.println(message + " " + seconds);
    }
}
