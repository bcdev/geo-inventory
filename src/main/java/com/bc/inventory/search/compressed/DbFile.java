package com.bc.inventory.search.compressed;

import com.bc.inventory.utils.S2Utils;
import com.bc.inventory.utils.S2Integer;
import com.google.common.geometry.S2Polygon;

import javax.imageio.stream.ImageInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Access methods to the file holding the entire geo-inventory database.
 * The file has a binary structure:
 */
class DbFile {

    static final String FILE_MARKER = "GEODB001";
    static final int DEFAULT_BLOCK_SIZE = 1000;

    static class Writer implements AutoCloseable {

        private final DataOutputStream dos;
        private final int blockSize;
        private final boolean useIndex;

        Writer(OutputStream os, boolean useIndex) {
            this(os, DEFAULT_BLOCK_SIZE,useIndex);
        }

        Writer(OutputStream os, int blockSize, boolean useIndex) {
            dos = new DataOutputStream(new BufferedOutputStream(os));
            this.blockSize = blockSize;
            this.useIndex = useIndex;
        }

        void write(List<DbFile.Entry> indexRecords, List<S2Integer.Coverage> bitmaps) throws IOException {
            writeHeader();
            writeIndex(indexRecords, bitmaps);
            int numBlocks = getNumBlocks(indexRecords.size(), blockSize);
            int[] blockSizes = new int[numBlocks];
            for (int i = 0; i < blockSizes.length; i++) {
                blockSizes[i] = calculateBlockSize(i, indexRecords);
            }
            writeBlockSizes(blockSizes);
            for (int i = 0; i < numBlocks; i++) {
                writeBlock(i, indexRecords);
            }
        }

        void writeHeader() throws IOException {
            dos.write(FILE_MARKER.getBytes());
        }

        void writeIndex(List<DbFile.Entry> indexRecords, List<S2Integer.Coverage> bitmaps) throws IOException {
            dos.writeInt(indexRecords.size());
            if(useIndex) {
                dos.writeInt(bitmaps.size());
            }
            for (DbFile.Entry record : indexRecords) {
                dos.writeInt(record.startTime);
            }
            for (DbFile.Entry record : indexRecords) {
                dos.writeInt(record.endTime);
            }
            if(useIndex) {
                for (DbFile.Entry record : indexRecords) {
                    dos.writeInt(record.coverageIndex);
                }
                for (S2Integer.Coverage s2Cover : bitmaps) {
                    dos.writeInt(s2Cover.intIds.length);
                }
                for (S2Integer.Coverage s2Cover : bitmaps) {
                    for (int intId : s2Cover.intIds) {
                        dos.writeInt(intId);
                    }
                }
            }
        }

        void writeBlockSizes(int[] blockSizes) throws IOException {
            for (int blockSize : blockSizes) {
                dos.writeInt(blockSize);
            }
        }

        private void writeBlock(int blockNumber, List<DbFile.Entry> entries) throws IOException {
            int startIndex = blockNumber * blockSize;
            int endIndex = Math.min(startIndex + blockSize, entries.size());

            byte[] compressPaths = compressPaths(entries, startIndex, endIndex);
            dos.writeInt(compressPaths.length);
            dos.write(compressPaths);
            for (int i = startIndex; i < endIndex; i++) {
                dos.writeInt(entries.get(i).polygonBytes.length);
            }
            for (int i = startIndex; i < endIndex; i++) {
                dos.write(entries.get(i).polygonBytes);
            }
        }

        private int calculateBlockSize(int blockNumber, List<DbFile.Entry> entries) throws IOException {
            int startIndex = blockNumber * blockSize;
            int endIndex = Math.min(startIndex + blockSize, entries.size());

            int bytesCompressedPath = 4 + compressPaths(entries, startIndex, endIndex).length;
            int bytesPolygonSizes = (endIndex - startIndex) * 4;
            int bytesPolygons = calculateSizePolygons(entries, startIndex, endIndex);
            return bytesCompressedPath + bytesPolygonSizes + bytesPolygons;
        }

        static int calculateSizePolygons(List<DbFile.Entry> entries, int startIndex, int endIndex) {
            int bytesPolygons = 0;
            for (int i = startIndex; i < endIndex; i++) {
                bytesPolygons += entries.get(i).polygonBytes.length;
            }
            return bytesPolygons;
        }

        @Override
        public void close() throws IOException {
            dos.close();
        }
    }

    static abstract class Reader implements AutoCloseable {

        private final int blockSize;
        private final boolean useIndex;
        private int numEntries;
        private int[] startTimes;
        private int[] endTimes;
        private int[] bitmapIds;
        private int[] bitmapSizes;
        private int currentEntryId = -1;
        private int currentBlockId = -1;
        private int[] blockSizes;
        private int[] blockOffsets;
        private String[] blockPath;
        private int[] blockPolgonSizes;
        private int[] blockPolgonOffsets;
        private ByteBuffer blockBB;
        private int currentEntryInBlock;
        private int[][] coverages;

        Reader(int blockSize, boolean useIndex) {
            this.blockSize = blockSize;
            this.useIndex = useIndex;
        }

        void readIndex() throws IOException {
            ByteBuffer bb = ByteBuffer.allocate(FILE_MARKER.length() + 4 + (useIndex ? 4 : 0));
            readFully(bb.array());
            if (!FILE_MARKER.equals(new String(bb.array(), 0, FILE_MARKER.length()))) {
                throw new IllegalArgumentException("file header does not match");
            }
            bb.position(FILE_MARKER.length());
            numEntries = bb.getInt();

            int numBitmaps = 0;
            if (useIndex) {
                numBitmaps = bb.getInt();
                bb = ByteBuffer.allocate(3 * 4 * numEntries + 4 * numBitmaps);
            } else {
                bb = ByteBuffer.allocate(2 * 4 * numEntries);
            }
            readFully(bb.array());

            IntBuffer intBuffer = bb.asIntBuffer();
            startTimes = new int[numEntries];
            intBuffer.get(startTimes);
            endTimes = new int[numEntries];
            intBuffer.get(endTimes);
            
            if (useIndex) {
                bitmapIds = new int[numEntries];
                intBuffer.get(bitmapIds);
                bitmapSizes = new int[numBitmaps];
                intBuffer.get(bitmapSizes);

                coverages = new int[numBitmaps][];
                for (int i = 0; i < coverages.length; i++) {
                    coverages[i] = readIntArray(bitmapSizes[i]);
                }
            }
            
            int numBlocks = getNumBlocks(numEntries, blockSize);
            blockSizes = readIntArray(numBlocks);
            blockOffsets = new int[blockSizes.length];
            int beginOfBlocks = getPosition();
            for (int i = 0; i < blockSizes.length; i++) {
                if (i == 0) {
                    blockOffsets[i] = beginOfBlocks;
                } else {
                    blockOffsets[i] = blockOffsets[i - 1] + blockSizes[i - 1];
                }
            }
        }

        int[] getStartTimes() {
            return startTimes;
        }

        int[] getEndTimes() {
            return endTimes;
        }

        int numBitmaps() {
            return coverages == null ? 0 : coverages.length;
        }

        int getBitmapIndex(int index) {
            return bitmapIds[index];
        }

        int[] getBitmap(int index) {
            return coverages[index];
        }

        void readEntry(int entryId) throws IOException {
            if (entryId == currentEntryId) {
                return;
            }
            int blockId = entryId / blockSize;
            if (blockId != currentBlockId) {
                // seek to block
                readBlock(blockId);
                currentBlockId = blockId;
            }
            currentEntryId = entryId;
            currentEntryInBlock = entryId % blockSize;
        }

        String getCurrentPath() {
            return blockPath[currentEntryInBlock];
        }

        byte[] getCurrentPolygonBytes() {
            blockBB.position(blockPolgonOffsets[currentEntryInBlock]);
            byte[] currentPolygonBytes = new byte[blockPolgonSizes[currentEntryInBlock]];
            blockBB.get(currentPolygonBytes);
            return currentPolygonBytes;
        }

        S2Polygon getCurrentPolygon() {
            blockBB.position(blockPolgonOffsets[currentEntryInBlock]);
            return S2Utils.asPolygon(blockBB);
        }

        private void readBlock(int blockId) throws IOException {
            seek(blockOffsets[blockId]);
            blockBB = ByteBuffer.allocate(blockSizes[blockId]);
            readFully(blockBB.array());

            int compressedPathSize = blockBB.getInt();
            blockPath = decompressStrings(blockBB.array(), 4, compressedPathSize);
            blockBB.position(blockBB.position() + compressedPathSize);

            int startIndex = blockId * blockSize;
            int endIndex = Math.min(startIndex + blockSize, numEntries);
            int numPolygonsInBlock = endIndex - startIndex;
            blockPolgonSizes = new int[numPolygonsInBlock];
            blockBB.asIntBuffer().get(blockPolgonSizes);
            blockBB.position(blockBB.position() + numPolygonsInBlock * 4);
            int blockBBPolygonStart = blockBB.position();
            blockPolgonOffsets = new int[blockPolgonSizes.length];
            for (int i = 0; i < blockPolgonSizes.length; i++) {
                if (i == 0) {
                    blockPolgonOffsets[i] = blockBBPolygonStart;
                } else {
                    blockPolgonOffsets[i] = blockPolgonOffsets[i - 1] + blockPolgonSizes[i - 1];
                }
            }
        }


        private int[] readIntArray(int numInts) throws IOException {
            ByteBuffer byteBuf = ByteBuffer.allocate(numInts * 4);
            readFully(byteBuf.array());
            IntBuffer intBuf = byteBuf.asIntBuffer();
            int[] result = new int[numInts];
            intBuf.get(result);
            return result;
        }
        
        abstract void readFully(byte[] b) throws IOException;
        
        abstract int getPosition() throws IOException;
        
        abstract void seek(int pos) throws IOException;
        
    }

    static class ImageInputStreamReader extends Reader {
        private final ImageInputStream iis;

        ImageInputStreamReader(ImageInputStream iis) {
            this(iis, DEFAULT_BLOCK_SIZE, true);
        }
        
        ImageInputStreamReader(ImageInputStream iis, boolean useIndex) {
            this(iis, DEFAULT_BLOCK_SIZE, useIndex);
        }

        ImageInputStreamReader(ImageInputStream iis, int blockSize, boolean useIndex) {
            super(blockSize, useIndex);
            this.iis = iis;
        }

        @Override
        void readFully(byte[] b) throws IOException {
            iis.readFully(b);
        }

        @Override
        int getPosition() throws IOException {
            return (int) iis.getStreamPosition();
        }

        @Override
        protected void seek(int pos) throws IOException {
           iis.seek(pos); 
        }

        @Override
        public void close() throws IOException {
            iis.close();
        }
    }

    static class InputStreamReader extends Reader {
        private final InputStream is;
        private int currentPos;

        InputStreamReader(InputStream is) {
            this(is, DEFAULT_BLOCK_SIZE, true);
        }
        
        InputStreamReader(InputStream is, boolean useIndex) {
            this(is, DEFAULT_BLOCK_SIZE, useIndex);
        }

        InputStreamReader(InputStream is, int blockSize, boolean useIndex) {
            super(blockSize, useIndex);
            this.is = is;
            this.currentPos = 0;
        }

        @Override
        void readFully(byte[] b) throws IOException {
            int off = 0;
            int len = b.length;
            while (len > 0) {
                int nbytes = is.read(b, off, len);
                if (nbytes == -1) {
                    throw new EOFException();
                }
                off += nbytes;
                len -= nbytes;
                currentPos += nbytes;
            }
        }
        
        @Override
        int getPosition() throws IOException {
            return currentPos;
        }
        
        @Override
        protected void seek(final int pos) throws IOException {
            if (pos < currentPos) {
                throw new IOException("Backwards seeking not supported when using InputStream based implementation.");
            }
            while (pos > currentPos){
                long skipped = is.skip(pos - currentPos);
                currentPos += skipped;
            }
        }

        @Override
        public void close() throws IOException {
            is.close();
        }
    }
       
    static class Entry {

        final int startTime;
        final int endTime;
        final String path;
        final byte[] polygonBytes;
        final int coverageIndex;

        Entry(int startTime, int endTime, String path, byte[] polygonBytes, int coverageIndex) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.path = path;
            this.polygonBytes = polygonBytes;
            this.coverageIndex = coverageIndex;
        }
    }

    static int getNumBlocks(int numEntries, int blockSize) {
        return (int) Math.ceil((float) numEntries / blockSize);
    }

    static byte[] compressPaths(List<DbFile.Entry> entries, int startIndex, int endIndex) throws IOException {
        String[] paths = new String[endIndex - startIndex];
        for (int entryIndex = startIndex, pathIndex = 0; entryIndex < endIndex; entryIndex++, pathIndex++) {
            paths[pathIndex] = entries.get(entryIndex).path;
        }
        return compressStrings(paths);
    }

    static byte[] compressStrings(String[] strings) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            sb.append(strings[i]);
            if (i < strings.length - 1) {
                sb.append('\t');
            }
        }
        byte[] bytesText = sb.toString().getBytes("UTF-8");
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        try (OutputStream out = new DeflaterOutputStream(baos1)) {
            out.write(bytesText);
        }
        return baos1.toByteArray();
    }

    static String[] decompressStrings(byte[] bytesCompressed, int offset, int compressedSize) throws IOException {
        try (InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytesCompressed, offset, compressedSize))) {
            ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) > 0) {
                baos2.write(buffer, 0, len);
            }
            String paths = new String(baos2.toByteArray(), "UTF-8");
            return paths.split("\t");
        }
    }


}
