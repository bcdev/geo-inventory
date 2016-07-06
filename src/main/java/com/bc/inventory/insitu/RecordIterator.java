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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator used in various implementations of the {@link RecordSource} interface.
 *
 * @author Norman
 */
public abstract class RecordIterator implements Iterator<Record> {
    private Record next;
    private boolean nextValid;

    public RecordIterator() {
    }

    @Override
    public boolean hasNext() {
        ensureValidNext();
        return next != null;
    }

    @Override
    public Record next() {
        ensureValidNext();
        if (next == null) {
            throw new NoSuchElementException();
        }
        nextValid = false;
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void ensureValidNext() {
        if (!nextValid) {
            next = getNextRecord();
            nextValid = true;
        }
    }

    /**
     * @return The next record, or {@code null} if there is no next record.
     */
    protected abstract Record getNextRecord();
}
