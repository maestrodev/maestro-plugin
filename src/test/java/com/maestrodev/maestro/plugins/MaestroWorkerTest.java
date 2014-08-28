/*
 * Copyright (c) 2013, MaestroDev. All rights reserved.
 */
package com.maestrodev.maestro.plugins;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.jr.ob.JSON;

/**
 * Unit test for MaestroWorker.
 */
public class MaestroWorkerTest {

    private MaestroWorker worker;
    private Map<String, Object> workitem;

    @Before
    public void before() {
        worker = new MaestroWorkerStub();
        workitem = new HashMap<String, Object>();
    }

    @Test
    public void testSetField() throws Exception {
        Map<String, Object> fields = new HashMap<String, Object>();
        workitem.put("fields", fields);
        worker.setWorkitem(workitem);

        worker.setField("some field", "some value");

        assertEquals(worker.getField("some field"), "some value");
    }

    @Test
    public void testGetArrayField() throws Exception {
        worker.setWorkitem(workitem);
        workitem.put("fields", new HashMap<String, Object>());

        String f = "test";

        // edge cases: null, empty array
        worker.setField(f, null);
        assertNull(worker.getArrayField(String.class, f));

        worker.setField(f, JSON.std.arrayFrom("[]"));
        assertEquals(Collections.emptyList(),
                worker.getArrayField(String.class, f));

        // string array
        List<String> expected = Arrays.asList(new String[] { "a", "b", "c" });

        worker.setField(f, JSON.std.arrayFrom("[\"a\", \"b\", \"c\"]"));
        assertEquals(expected, worker.getArrayField(String.class, f));

        // integer array
        worker.setField(f, JSON.std.arrayFrom("[1, 2, 3]"));
        // TODO it is returning integers as longs
        // List<Integer> actualInt = worker.getArrayField( Integer.class, f );
        // assertArrayEquals( new Integer[] { 1, 2, 3 }, actualInt.toArray() );

        List<Long> actualInt = worker.getArrayField(Long.class, f);
        assertArrayEquals(new Integer[] { 1, 2, 3 }, actualInt.toArray());

        worker.setField(f, JSON.std.arrayFrom("[1.0, 2.0, 3.0]"));
        List<Double> actualDouble = worker.getArrayField(Double.class, f);
        assertArrayEquals(new Double[] { 1.0, 2.0, 3.0 },
                actualDouble.toArray());
    }

    @Test
    public void testExceptionOnPerform() throws Exception {
        workitem.put("fields", new HashMap<String, Object>());
        worker.perform("fail", workitem);
        assertTrue(
                worker.getError(),
                worker.getError()
                        .startsWith(
                                "Plugin com.maestrodev.maestro.plugins.MaestroWorkerTest$MaestroWorkerStub.fail failed: java.lang.Exception: exception"));
    }

    @Test
    public void testPerform() throws Exception {
        workitem.put("fields", new HashMap<String, Object>());
        worker.perform("test", workitem);

        String expected = "Executing plugin: com.maestrodev.maestro.plugins.MaestroWorkerTest$MaestroWorkerStub.test\n"
                + "Finished plugin execution: com.maestrodev.maestro.plugins.MaestroWorkerTest$MaestroWorkerStub.test\n";
        assertEquals(expected, ((MaestroWorkerStub) worker).output.toString());
        assertNull(worker.getError());
    }

    @Test
    public void testParseBigDecimal() {
        Map<String,Object> map = new HashMap<String, Object>();
        Map<String,Object> fields = new HashMap<String, Object>();
        fields.put("big", new BigInteger("16740918963672507888"));
        map.put("fields", fields);
        worker.perform("getError", map);
        assertNull(worker.getError());
    }

    class MaestroWorkerStub extends MaestroWorker {
        public StringBuilder output = new StringBuilder();

        public void test() throws Exception {
            // do nothing
        }

        public void fail() throws Exception {
            throw new Exception("exception");
        }

        @Override
        public void writeOutput(String output) {
            this.output.append(output);
        }

    }

}
