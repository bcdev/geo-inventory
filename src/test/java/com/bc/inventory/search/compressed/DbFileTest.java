package com.bc.inventory.search.compressed;

import com.bc.inventory.utils.S2Integer;
import org.junit.Before;
import org.junit.Test;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DbFileTest {
    
    private DbFile.Entry[] e;
    private S2Integer.Coverage roaringBitmap;

    @Before
    public void setUp() throws Exception {
        e = new DbFile.Entry[8];
        for (int i = 0; i < e.length; i++) {
            byte[] polygonBytes = {(byte) i, (byte) (i+1), (byte) (i+2), (byte) (i+3)};
            e[i] = new DbFile.Entry(i, i+5,  "p"+i, polygonBytes, 0);
        }
        roaringBitmap = new S2Integer.Coverage(2, 4, 6, 8);
    }

    @Test
    public void testWriteHeader() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        DbFile.Writer writer = new DbFile.Writer(baos, true);
        writer.writeHeader();
        writer.close();

        assertEquals(DbFile.FILE_MARKER, new String(baos.toByteArray()));
    }

    @Test
    public void testWriteRead_1() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(DbFile.Writer writer = new DbFile.Writer(baos, true)) {
            List<DbFile.Entry> entries = Collections.singletonList(e[0]);
            List<S2Integer.Coverage> bitmaps = Collections.singletonList(roaringBitmap);
            writer.write(entries, bitmaps);
        }
        byte[] bytes = baos.toByteArray();

        ImageInputStream iis = new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes));
        try (DbFile.Reader reader = new DbFile.Reader(iis, true)) {
            reader.readIndex();
            assertArrayEquals(new int[]{0}, reader.getStartTimes());
            assertArrayEquals(new int[]{5}, reader.getEndTimes());
            assertEquals(0, reader.getBitmapIndex(0));
            assertEquals(1, reader.numBitmaps());
            assertArrayEquals(roaringBitmap.intIds, reader.getBitmap(0));

            testEntry(reader, 0);
        }
    }

    @Test
    public void testWriteRead_2() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(DbFile.Writer writer = new DbFile.Writer(baos, true)) {
            List<DbFile.Entry> entries = Arrays.asList(e[0], e[1]);
            List<S2Integer.Coverage> bitmaps = Collections.singletonList(roaringBitmap);
            writer.write(entries, bitmaps);
        }
        byte[] bytes = baos.toByteArray();

        try (DbFile.Reader reader = new DbFile.Reader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), true)) {
            reader.readIndex();
            assertArrayEquals(new int[]{0,1}, reader.getStartTimes());
            assertArrayEquals(new int[]{5,6}, reader.getEndTimes());
            assertEquals(0, reader.getBitmapIndex(0));
            assertEquals(0, reader.getBitmapIndex(1));
            assertEquals(1, reader.numBitmaps());
            assertArrayEquals(roaringBitmap.intIds, reader.getBitmap(0));

            testEntry(reader, 0);
        }

        try (DbFile.Reader reader = new DbFile.Reader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), true)) {
            reader.readIndex();
            testEntry(reader, 1);
        }

        try (DbFile.Reader reader = new DbFile.Reader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), true)) {
            reader.readIndex();
            testEntry(reader, 0);
            testEntry(reader, 1);
        }
    }

    @Test
    public void testWriteRead_multiBlocks() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(DbFile.Writer writer = new DbFile.Writer(baos, 3, true)) {
            List<DbFile.Entry> entries = Arrays.asList(e);
            List<S2Integer.Coverage> bitmaps = Collections.singletonList(roaringBitmap);
            writer.write(entries, bitmaps);
        }
        byte[] bytes = baos.toByteArray();

        try (DbFile.Reader reader = new DbFile.Reader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), 3, true)) {
            reader.readIndex();
            assertArrayEquals(new int[]{0,1,2,3,4,5,6,7}, reader.getStartTimes());
            assertArrayEquals(new int[]{5,6,7,8,9,10,11,12}, reader.getEndTimes());
            for (int i = 0; i < 8; i++) {
                assertEquals(0, reader.getBitmapIndex(i));
            }
            assertEquals(1, reader.numBitmaps());
            assertArrayEquals(roaringBitmap.intIds, reader.getBitmap(0));

            testEntry(reader, 0);
            testEntry(reader, 1);
            testEntry(reader, 2);
            testEntry(reader, 3);
            testEntry(reader, 4);
            testEntry(reader, 5);
            testEntry(reader, 6);
            testEntry(reader, 7);
        }

        try (DbFile.Reader reader = new DbFile.Reader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), 3, true)) {
            reader.readIndex();
            testEntry(reader, 5);
            testEntry(reader, 6);
            testEntry(reader, 7);
        }

    }

    private void testEntry(DbFile.Reader reader, int entryId) throws IOException {
        reader.readEntry(entryId);
        assertEquals(e[entryId].path, reader.getCurrentPath());
        assertArrayEquals(e[entryId].polygonBytes, reader.getCurrentPolygonBytes());
    }

    @Test
    public void testNumBlocks() throws Exception {
        assertEquals(0, DbFile.getNumBlocks(0, DbFile.DEFAULT_BLOCK_SIZE));
        assertEquals(1, DbFile.getNumBlocks(1, DbFile.DEFAULT_BLOCK_SIZE));
        assertEquals(1, DbFile.getNumBlocks(999, DbFile.DEFAULT_BLOCK_SIZE));
        assertEquals(1, DbFile.getNumBlocks(1000, DbFile.DEFAULT_BLOCK_SIZE));
        assertEquals(2, DbFile.getNumBlocks(1001, DbFile.DEFAULT_BLOCK_SIZE));

    }
}