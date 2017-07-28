package com.bc.inventory.search.compressed;

/**
 * An entry in the geo inventory
 */
class Entry {

    final int startTime;
    final int endTime;
    final int bitmapId;
    final String path;
    final byte[] polygonBytes;

    Entry(int startTime, int endTime, int bitmapId, String path, byte[] polygonBytes) {
        this.path = path;
        this.startTime = startTime;
        this.endTime = endTime;
        this.bitmapId = bitmapId;
        this.polygonBytes = polygonBytes;
    }
}
