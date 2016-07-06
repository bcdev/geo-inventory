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

import java.util.Arrays;
import java.util.List;

/**
 * A default implementation of a {@link Header}.
 *
 * @author Norman
 */
public class DefaultHeader implements Header {

    public static final String ANNOTATION_EXCLUSION_REASON = "ExclusionReason";

    private final boolean hasLocation;
    private final boolean hasTime;
    private final List<String> attributeNames;
    private final List<String> annotationNames;

    public DefaultHeader(boolean hasLocation, boolean hasTime, String... attributeNames) {
        this(hasLocation, hasTime, attributeNames, new String[]{ANNOTATION_EXCLUSION_REASON});
    }

    public DefaultHeader(boolean hasLocation, boolean hasTime, String[] attributeNames, String[] annotationNames) {
        this.hasLocation = hasLocation;
        this.hasTime = hasTime;
        this.attributeNames = Arrays.asList(attributeNames);
        this.annotationNames = Arrays.asList(annotationNames);
    }

    @Override
    public boolean hasLocation() {
        return hasLocation;
    }

    @Override
    public boolean hasTime() {
        return hasTime;
    }

    @Override
    public String[] getAttributeNames() {
        return attributeNames.toArray(new String[attributeNames.size()]);
    }

    @Override
    public int getAttributeIndex(String name) {
        return attributeNames.indexOf(name);
    }

    @Override
    public String[] getAnnotationNames() {
        return annotationNames.toArray(new String[annotationNames.size()]);
    }

    @Override
    public int getAnnotationIndex(String name) {
        return annotationNames.indexOf(name);
    }

    @Override
    public String getAttributeName(int index) {
        if (index >= 0) {
            return attributeNames.get(index);
        } else {
            return null;
        }
    }
}
