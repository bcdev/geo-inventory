package com.bc.inventory.utils;

import com.google.common.geometry.S2CellId;
import org.junit.Test;

import static org.junit.Assert.*;

public class S2IntegerTest {

    @Test
    public void testConversion() throws Exception {
        S2CellId s2_0 = S2CellId.fromFacePosLevel(0,0,0);
        System.out.println("s2_0 = " + s2_0);
        int asInt = S2Integer.asIntAtLevel(s2_0, 0);
        System.out.println("i = " + asInt);
        assertEquals(0, asInt);

    }
}