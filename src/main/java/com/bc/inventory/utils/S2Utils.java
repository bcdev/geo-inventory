package com.bc.inventory.utils;

import com.google.common.geometry.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Methods to interact with S2 objects
 */
public class S2Utils {

    public static byte[] asBytes(S2Polygon polygon) throws IOException {
        S2Loop loop = polygon.loop(0);
        int numVertices = loop.numVertices();
        final int numBytes = 4 + numVertices * 3 * 4 + 4 * 4 + 4 + 1;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(numVertices);

            for (int i = 0; i < numVertices; i++) {
                S2Point vertex = loop.vertex(i);
                dos.writeFloat((float) vertex.getX());
                dos.writeFloat((float) vertex.getY());
                dos.writeFloat((float) vertex.getZ());
            }

            S2LatLngRect bound = loop.getRectBound();
            float latLo = (float) bound.lat().lo();
            float latHi = (float) bound.lat().hi();
            float lngLo = (float) bound.lng().lo();
            float lngHi = (float) bound.lng().hi();
            dos.writeFloat(latLo);
            dos.writeFloat(latHi);
            dos.writeFloat(lngLo);
            dos.writeFloat(lngHi);

            int firstLogicalVertex = loop.getFirstLogicalVertex();
            dos.writeInt(firstLogicalVertex);
            boolean originInside = loop.isOriginInside();
            dos.writeBoolean(originInside);
        }
        return baos.toByteArray();
    }

    public static S2Polygon asPolygon(byte[] polygonBytes) {
        return asPolygon(ByteBuffer.wrap(polygonBytes));
    }

    public static S2Polygon asPolygon(ByteBuffer bb) {
        int numLoopPoints = bb.getInt();

        S2Point[] vertices = new S2Point[numLoopPoints];
        for (int i = 0; i < numLoopPoints; i++) {
            double x = bb.getFloat();
            double y = bb.getFloat();
            double z = bb.getFloat();
            vertices[i] = new S2Point(x, y, z);
        }
        double latLo = bb.getFloat();
        double latHi = bb.getFloat();
        double lngLo = bb.getFloat();
        double lngHi = bb.getFloat();
        R1Interval lat = new R1Interval(latLo, latHi);
        S1Interval lng = new S1Interval(lngLo, lngHi);
        S2LatLngRect bound = new S2LatLngRect(lat, lng);

        int firstLogicalVertex = bb.getInt();
        boolean originInside = (bb.get() == 1);
        S2Loop loop = new S2Loop(vertices, bound, firstLogicalVertex, originInside);

        return new S2Polygon(loop);
    }
}
