package com.bc.inventory.search.coverage;

import com.bc.inventory.utils.S2Integer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

/**
 * For reading and writing the 'index' file
 */
class IndexFile {

    static class Writer implements AutoCloseable {

        private final DataOutputStream dos;

        Writer(OutputStream os) {
            dos = new DataOutputStream(new BufferedOutputStream(os));
        }

        void writeRecords(List<IndexCreator.IndexRecord> indexRecords) throws IOException {
            dos.writeInt(indexRecords.size());
            for (IndexCreator.IndexRecord record : indexRecords) {
                dos.writeInt(record.startTime);
            }
            for (IndexCreator.IndexRecord record : indexRecords) {
                dos.writeInt(record.endTime);
            }
            for (IndexCreator.IndexRecord record : indexRecords) {
                dos.writeInt(record.coverageId);
            }
            for (IndexCreator.IndexRecord record : indexRecords) {
                dos.writeInt(record.dataOffset);
            }
        }

        void writeCoverages(List<S2Integer.Coverage> coverages) throws IOException {
            dos.writeInt(coverages.size());
            for (S2Integer.Coverage s2Cover : coverages) {
                dos.writeInt(s2Cover.intIds.length);
                for (int intId : s2Cover.intIds) {
                    dos.writeInt(intId);
                }
            }
        }

        @Override
        public void close() throws IOException {
            dos.close();
        }
    }

    static class Reader implements AutoCloseable {

        private final DataInputStream dis;
        private int[] startTimes;
        private int[] endTimes;
        private int[] coverageIndices;
        private int[] dataOffsets;

        Reader(InputStream is) {
            dis = new DataInputStream(new BufferedInputStream(is));
        }

        void readRecords() throws IOException {
            int numRecords = dis.readInt();
            startTimes = readIntArray(numRecords);
            endTimes = readIntArray(numRecords);
            coverageIndices = readIntArray(numRecords);
            dataOffsets = readIntArray(numRecords);
        }

        int[] getStartTimes() {
            return startTimes;
        }

        int[] getEndTimes() {
            return endTimes;
        }

        int[] getCoverageIndices() {
            return coverageIndices;
        }

        int[] getDataOffsets() {
            return dataOffsets;
        }

        int[][] readCoverages() throws IOException {
            int[][] coverages = new int[dis.readInt()][0];
            for (int i = 0; i < coverages.length; i++) {
                coverages[i] = readCoverage();
            }
            return coverages;
        }

        private int[] readCoverage() throws IOException {
            return readIntArray(dis.readInt());
        }

        private int[] readIntArray(int numInts) throws IOException {
            ByteBuffer byteBuf = ByteBuffer.allocate(numInts * 4);
            dis.readFully(byteBuf.array());
            IntBuffer intBuf = byteBuf.asIntBuffer();
            int[] result = new int[numInts];
            intBuf.get(result);
            return result;
        }

        @Override
        public void close() throws IOException {
            dis.close();
        }
    }
}
