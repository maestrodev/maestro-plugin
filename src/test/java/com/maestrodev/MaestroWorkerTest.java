package com.maestrodev;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

/**
 * Unit test for MaestroWorker.
 */
public class MaestroWorkerTest
{

    @SuppressWarnings( "unchecked" )
    @Test
    public void testSetField()
        throws Exception
    {
        JSONObject workitem = new JSONObject();
        JSONObject fields = new JSONObject();
        workitem.put( "fields", fields );

        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem( workitem );

        worker.setField( "some field", "some value" );

        assertEquals( worker.getField( "some field" ), "some value" );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void testGetArrayField()
        throws Exception
    {
        JSONObject workitem = new JSONObject();
        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem( workitem );
        workitem.put( "fields", new JSONObject() );

        String f = "test";

        JSONParser parser = new JSONParser();

        // edge cases: null, empty array
        worker.setField( f, null );
        assertNull( worker.getArrayField( String.class, f ) );

        worker.setField( f, parser.parse( "[]" ) );
        assertEquals( Collections.emptyList(), worker.getArrayField( String.class, f ) );

        // string array
        List<String> expected = Arrays.asList( new String[] { "a", "b", "c" } );

        worker.setField( f, parser.parse( "[\"a\", \"b\", \"c\"]" ) );
        assertEquals( expected, worker.getArrayField( String.class, f ) );

        // integer array
        worker.setField( f, parser.parse( "[1, 2, 3]" ) );
        // TODO it is returning integers as longs
        // List<Integer> actualInt = worker.getArrayField( Integer.class, f );
        // assertArrayEquals( new Integer[] { 1, 2, 3 }, actualInt.toArray() );

        List<Long> actualInt = worker.getArrayField( Long.class, f );
        assertArrayEquals( new Long[] { 1l, 2l, 3l }, actualInt.toArray() );

        worker.setField( f, parser.parse( "[1.0, 2.0, 3.0]" ) );
        List<Double> actualDouble = worker.getArrayField( Double.class, f );
        assertArrayEquals( new Double[] { 1.0, 2.0, 3.0 }, actualDouble.toArray() );
    }

    @Test
    public void testExceptionOnPerform()
        throws Exception
    {
        MaestroWorker worker = new MaestroWorkerStub()
        {
            public void test()
                throws Exception
            {
                throw new Exception( "exception" );
            }
        };
        JSONObject workitem = new JSONObject();
        workitem.put( "fields", new JSONObject() );
        worker.perform( "test", workitem );
        assertTrue( worker.getError(),
                    worker.getError().startsWith( "Plugin com.maestrodev.MaestroWorkerTest$1.test failed: java.lang.Exception: exception" ) );
    }

    @Test
    public void testPerform()
        throws Exception
    {
        MaestroWorkerStub worker = new MaestroWorkerStub();
        JSONObject workitem = new JSONObject();
        workitem.put( "fields", new JSONObject() );
        worker.perform( "test", workitem );

        String expected =
            "Executing plugin: com.maestrodev.MaestroWorkerTest$MaestroWorkerStub.test\n"
                + "Finished plugin execution: com.maestrodev.MaestroWorkerTest$MaestroWorkerStub.test\n";
        assertEquals( expected, worker.output.toString() );
        assertNull( worker.getError() );
    }

    class MaestroWorkerStub
        extends MaestroWorker
    {
        public StringBuilder output = new StringBuilder();

        public void test()
            throws Exception
        {
            // do nothing
        }

        @Override
        public void writeOutput( String output )
        {
            this.output.append( output );
        }

    }
}
