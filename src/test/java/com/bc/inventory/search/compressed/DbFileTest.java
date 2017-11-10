package com.bc.inventory.search.compressed;

import com.bc.inventory.utils.S2Integer;
import org.junit.Before;
import org.junit.Test;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DbFileTest {
    
    private DbFile.Entry[] e;
    private S2Integer.Coverage s2Coverage;

    @Before
    public void setUp() throws Exception {
        e = new DbFile.Entry[8];
        for (int i = 0; i < e.length; i++) {
            byte[] polygonBytes = {(byte) i, (byte) (i+1), (byte) (i+2), (byte) (i+3)};
            e[i] = new DbFile.Entry(i, i+5,  "p"+i, polygonBytes, 0);
        }
        s2Coverage = new S2Integer.Coverage(2, 4, 6, 8);
    }

    @Test
    public void testWriteHeader_noContent() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        DbFile.Writer writer = new DbFile.Writer(baos, true);
        writer.writeHeader();
        writer.close();

        assertEquals(DbFile.FILE_MARKER, new String(baos.toByteArray()));
    }

    @Test
    public void testWriteRead_oneEntry() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(DbFile.Writer writer = new DbFile.Writer(baos, true)) {
            List<DbFile.Entry> entries = Collections.singletonList(e[0]);
            List<S2Integer.Coverage> bitmaps = Collections.singletonList(s2Coverage);
            writer.write(entries, bitmaps);
        }
        byte[] bytes = baos.toByteArray();

        ImageInputStream iis = new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes));
        try (DbFile.Reader reader = new DbFile.ImageInputStreamReader(iis)) {
            assertReader_oneEntry(reader);
        }
        InputStream is = new ByteArrayInputStream(bytes);
        try (DbFile.Reader reader = new DbFile.InputStreamReader(is)) {
            assertReader_oneEntry(reader);
        }
    }

    private void assertReader_oneEntry(DbFile.Reader reader) throws IOException {
        reader.readIndex();
        assertArrayEquals(new int[]{0}, reader.getStartTimes());
        assertArrayEquals(new int[]{5}, reader.getEndTimes());
        assertEquals(0, reader.getBitmapIndex(0));
        assertEquals(1, reader.numBitmaps());
        assertArrayEquals(s2Coverage.intIds, reader.getBitmap(0));

        assertSingleEntry(reader, 0);
    }

    @Test
    public void testWriteRead_twoEntries() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(DbFile.Writer writer = new DbFile.Writer(baos, true)) {
            List<DbFile.Entry> entries = Arrays.asList(e[0], e[1]);
            List<S2Integer.Coverage> bitmaps = Collections.singletonList(s2Coverage);
            writer.write(entries, bitmaps);
        }
        byte[] bytes = baos.toByteArray();

        try (DbFile.Reader reader = createIISR(bytes)) {
            assertReader_twoEntries_all(reader);
        }
        try (DbFile.Reader reader = createIISR(bytes)) {
            assertReader_twoEntries_readOne(reader, 1);
        }
        try (DbFile.Reader reader = createIISR(bytes)) {
            assertReader_twoEntries_readOne(reader, 0);
        }
        
        try (DbFile.Reader reader = createISR(bytes)) {
            assertReader_twoEntries_all(reader);
        }
        try (DbFile.Reader reader = createISR(bytes)) {
            assertReader_twoEntries_readOne(reader, 1);
        }
        try (DbFile.Reader reader = createISR(bytes)) {
            assertReader_twoEntries_readOne(reader, 0);
        }
    }

    private DbFile.InputStreamReader createISR(byte[] bytes) {
        return new DbFile.InputStreamReader(new ByteArrayInputStream(bytes));
    }

    private DbFile.ImageInputStreamReader createIISR(byte[] bytes) {
        return new DbFile.ImageInputStreamReader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)));
    }

    private void assertReader_twoEntries_readOne(DbFile.Reader reader, int entryId) throws IOException {
        reader.readIndex();
        assertSingleEntry(reader, entryId);
    }

    private void assertReader_twoEntries_all(DbFile.Reader reader) throws IOException {
        reader.readIndex();
        assertArrayEquals(new int[]{0,1}, reader.getStartTimes());
        assertArrayEquals(new int[]{5,6}, reader.getEndTimes());
        assertEquals(0, reader.getBitmapIndex(0));
        assertEquals(0, reader.getBitmapIndex(1));
        assertEquals(1, reader.numBitmaps());
        assertArrayEquals(s2Coverage.intIds, reader.getBitmap(0));

        assertSingleEntry(reader, 0);
        assertSingleEntry(reader, 1);
        assertSingleEntry(reader, 1);
    }

    @Test
    public void testWriteRead_multiBlocks() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(DbFile.Writer writer = new DbFile.Writer(baos, 3, true)) {
            List<DbFile.Entry> entries = Arrays.asList(e);
            List<S2Integer.Coverage> bitmaps = Collections.singletonList(s2Coverage);
            writer.write(entries, bitmaps);
        }
        byte[] bytes = baos.toByteArray();

        try (DbFile.Reader reader = new DbFile.ImageInputStreamReader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), 3, true)) {
            assertReader_multiBlocks_all(reader);
        }
        try (DbFile.Reader reader = new DbFile.ImageInputStreamReader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), 3, true)) {
            assertReader_multiBlocks_laterOnes(reader);
        }
        try (DbFile.Reader reader = new DbFile.ImageInputStreamReader(new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes)), 3, true)) {
            assertReader_multiBlocks_blocksReverse(reader);
        }
        
        try (DbFile.Reader reader = new DbFile.InputStreamReader(new ByteArrayInputStream(bytes), 3, true)) {
            assertReader_multiBlocks_all(reader);
        }
        try (DbFile.Reader reader = new DbFile.InputStreamReader(new ByteArrayInputStream(bytes), 3, true)) {
            assertReader_multiBlocks_laterOnes(reader);
        }
        try (DbFile.Reader reader = new DbFile.InputStreamReader(new ByteArrayInputStream(bytes), 3, true)) {
            assertReader_multiBlocks_blocksReverse(reader);
            fail();
        } catch (IOException ioe) {
            assertEquals("Backwards seeking not supported when using InputStream based implementation.", ioe.getMessage());
        }
    }

    private void assertReader_multiBlocks_all(DbFile.Reader reader) throws IOException {
        reader.readIndex();
        assertArrayEquals(new int[]{0,1,2,3,4,5,6,7}, reader.getStartTimes());
        assertArrayEquals(new int[]{5,6,7,8,9,10,11,12}, reader.getEndTimes());
        for (int i = 0; i < 8; i++) {
            assertEquals(0, reader.getBitmapIndex(i));
        }
        assertEquals(1, reader.numBitmaps());
        assertArrayEquals(s2Coverage.intIds, reader.getBitmap(0));

        assertSingleEntry(reader, 0);
        assertSingleEntry(reader, 1);
        assertSingleEntry(reader, 2);
        assertSingleEntry(reader, 3);
        assertSingleEntry(reader, 4);
        assertSingleEntry(reader, 5);
        assertSingleEntry(reader, 6);
        assertSingleEntry(reader, 7);
    }

    private void assertReader_multiBlocks_laterOnes(DbFile.Reader reader) throws IOException {
        reader.readIndex();
        assertSingleEntry(reader, 5);
        assertSingleEntry(reader, 6);
        assertSingleEntry(reader, 7);
    }

    private void assertReader_multiBlocks_blocksReverse(DbFile.Reader reader) throws IOException {
        reader.readIndex();
        assertSingleEntry(reader, 7);
        assertSingleEntry(reader, 0);
    }
    
    private void assertSingleEntry(DbFile.Reader reader, int entryId) throws IOException {
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