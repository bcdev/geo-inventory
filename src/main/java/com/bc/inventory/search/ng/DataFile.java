package com.bc.inventory.search.ng;

import com.google.common.geometry.R1Interval;
import com.google.common.geometry.S1Interval;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * For reading and writing the 'data' file
 */
class DataFile {

    static class Writer implements AutoCloseable {

        private final DataOutputStream dos;

        Writer(OutputStream os) {
            dos = new DataOutputStream(new BufferedOutputStream(os));
        }

        int writeRecord(S2Polygon s2Polygon, String path) throws IOException {
            int pos = dos.size();
            writePolygon(s2Polygon, dos);
            dos.writeUTF(path);
            return pos;
        }

        int writeRecord(byte[] s2PolygonBytes, String path) throws IOException {
            int pos = dos.size();
            dos.write(s2PolygonBytes);
            dos.writeUTF(path);
            return pos;
        }

        @Override
        public void close() throws IOException {
            dos.close();
        }
    }

    static class Reader implements AutoCloseable {

        private final DataInputStream dis;
        private int pos;

        Reader(InputStream is) {
            dis = new DataInputStream(new BufferedInputStream(is));
            pos = 0;
        }

        void seekTo(int dataOffset) throws IOException {
            dis.skipBytes(dataOffset - pos);
            pos = dataOffset;
        }

        S2Polygon readPolygon() throws IOException {
            final int numLoopPoints = dis.readInt();
            final int numLoopBytes = numLoopPoints * 3 * 8 + 4 * 8 + 4 + 1;
            final byte[] loopBytes = new byte[numLoopBytes];
            dis.readFully(loopBytes, 0, numLoopBytes);
            pos += 4 + numLoopBytes;
            return createS2Polygon(loopBytes, numLoopPoints);
        }

        String readPath() throws IOException {
            String path = dis.readUTF();
            pos += path.length() + 2;
            return path;
        }

        @Override
        public void close() throws IOException {
            dis.close();
        }
    }

    private static void writePolygon(S2Polygon s2Polygon, DataOutputStream dos) throws IOException {
        S2Loop loop = s2Polygon.loop(0);
        int numVertices = loop.numVertices();
        writePolygon(loop, numVertices, dos);
    }

    static byte[] polygon2byte(S2Polygon s2Polygon) throws IOException {
        S2Loop loop = s2Polygon.loop(0);
        int numVertices = loop.numVertices();
        final int numBytes = 4 + numVertices * 3 * 8 + 4 * 8 + 4 + 1;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(numBytes);
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            writePolygon(loop, numVertices, dos);
        }
        return baos.toByteArray();
    }

    private static void writePolygon(S2Loop loop, int numVertices, DataOutputStream dos) throws IOException {
        dos.writeInt(numVertices);

        for (int i = 0; i < numVertices; i++) {
            S2Point vertex = loop.vertex(i);
            dos.writeDouble(vertex.getX());
            dos.writeDouble(vertex.getY());
            dos.writeDouble(vertex.getZ());
        }

        S2LatLngRect bound = loop.getRectBound();
        double latLo = bound.lat().lo();
        double latHi = bound.lat().hi();
        double lngLo = bound.lng().lo();
        double lngHi = bound.lng().hi();
        dos.writeDouble(latLo);
        dos.writeDouble(latHi);
        dos.writeDouble(lngLo);
        dos.writeDouble(lngHi);

        int firstLogicalVertex = loop.getFirstLogicalVertex();
        dos.writeInt(firstLogicalVertex);
        boolean originInside = loop.isOriginInside();
        dos.writeBoolean(originInside);
    }

    private static S2Polygon createS2Polygon(byte[] loopByte, int numLoopPoints) {
        ByteBuffer bb = ByteBuffer.wrap(loopByte);

        S2Point[] vertices = new S2Point[numLoopPoints];
        for (int i = 0; i < numLoopPoints; i++) {
            double x = bb.getDouble();
            double y = bb.getDouble();
            double z = bb.getDouble();
            vertices[i] = new S2Point(x, y, z);
        }
        double latLo = bb.getDouble();
        double latHi = bb.getDouble();
        double lngLo = bb.getDouble();
        double lngHi = bb.getDouble();
        R1Interval lat = new R1Interval(latLo, latHi);
        S1Interval lng = new S1Interval(lngLo, lngHi);
        S2LatLngRect bound = new S2LatLngRect(lat, lng);

        int firstLogicalVertex = bb.getInt();
        boolean originInside = (bb.get() == 1);
        S2Loop loop = new S2Loop(vertices, bound, firstLogicalVertex, originInside);

        return new S2Polygon(loop);
    }

    static class Reader2 implements AutoCloseable {

        private final DataInputStream dis;
        private final int size;
        private int pos;
        private byte[] byteBuffer;
        private DataInput bb;

        Reader2(InputStream is) throws IOException {
            size = (int) ((FileInputStream) is).getChannel().size();
            dis = new DataInputStream(new BufferedInputStream(is));
            pos = 0;
        }

        void seekTo(int dataOffset, int length) throws IOException {
            if (length == -1) {
                length = size - dataOffset;
            }
            dis.skipBytes(dataOffset - pos);
            byteBuffer = new byte[length];
            dis.readFully(byteBuffer);
            bb = new DataInputStream(new ByteArrayInputStream(byteBuffer));
            pos = dataOffset + length;
        }

        S2Polygon readPolygon() throws IOException {
            final int numLoopPoints = bb.readInt();
            S2Point[] vertices = new S2Point[numLoopPoints];
            for (int i = 0; i < numLoopPoints; i++) {
                double x = bb.readDouble();
                double y = bb.readDouble();
                double z = bb.readDouble();
                vertices[i] = new S2Point(x, y, z);
            }
            double latLo = bb.readDouble();
            double latHi = bb.readDouble();
            double lngLo = bb.readDouble();
            double lngHi = bb.readDouble();
            R1Interval lat = new R1Interval(latLo, latHi);
            S1Interval lng = new S1Interval(lngLo, lngHi);
            S2LatLngRect bound = new S2LatLngRect(lat, lng);

            int firstLogicalVertex = bb.readInt();
            boolean originInside = bb.readBoolean();
            S2Loop loop = new S2Loop(vertices, bound, firstLogicalVertex, originInside);

            return new S2Polygon(loop);
        }

        String readPath() throws IOException {
            return bb.readUTF();
        }

        @Override
        public void close() throws IOException {
            dis.close();
        }
    }
}
