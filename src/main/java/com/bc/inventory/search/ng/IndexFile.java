package com.bc.inventory.search.ng;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * For reading and writing the 'index' file
 */
class IndexFile {

    static class Writer implements AutoCloseable {

        private final DataOutputStream dos;

        Writer(OutputStream os) {
            dos = new DataOutputStream(new BufferedOutputStream(os));
        }

        void writeNumRecords(int numRecords) throws IOException {
            dos.writeInt(numRecords);
        }

        void writeRecord(int startTime, int endTime, int coverageId, int dataOffset) throws IOException {
            dos.writeInt(startTime);
            dos.writeInt(endTime);
            dos.writeInt(coverageId);
            dos.writeInt(dataOffset);
        }

        void writeCoverage(int[] intIds) throws IOException {
            dos.writeInt(intIds.length);
            for (int intId : intIds) {
                dos.writeInt(intId);
            }
        }

        @Override
        public void close() throws IOException {
            dos.close();
        }
    }

    static class Reader implements AutoCloseable {

        private final DataInputStream dis;

        Reader(InputStream is) {
            dis = new DataInputStream(new BufferedInputStream(is));
        }

        int readNumRecords() throws IOException {
            return dis.readInt();
        }

        CoverageIndex.IndexRecord readIndexRecord() throws IOException {
            return new CoverageIndex.IndexRecord(
                    dis.readInt(),
                    dis.readInt(),
                    dis.readInt(),
                    dis.readInt()
            );
        }

        int[] readCoverage() throws IOException {
            int[] coverage = new int[dis.readInt()];
            for (int j = 0; j < coverage.length; j++) {
                coverage[j] = dis.readInt();
            }
            return coverage;
        }

        @Override
        public void close() throws IOException {
            dis.close();
        }
    }

}
