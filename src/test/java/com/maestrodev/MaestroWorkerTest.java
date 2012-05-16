package com.maestrodev;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;

/**
 * Unit test for MaestroWorker.
 */
public class MaestroWorkerTest
{
    
    @SuppressWarnings( "unchecked" )
    @Test
    public void testSetField() throws Exception
    {
        JSONObject workitem = new JSONObject();
        JSONObject fields = new JSONObject();
        workitem.put("fields", fields);
        
        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        
        worker.setField("some field", "some value");
       
        assertEquals(worker.getField("some field"), "some value");
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void testGetArrayField()
        throws Exception
    {
        JSONObject workitem = new JSONObject();
        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        workitem.put( "fields", new JSONObject() );

        String f = "test";

        // string array
        List<String> expected = Arrays.asList( new String[] { "a", "b", "c" } );

        JSONArray array = new JSONArray();
        array.addAll( expected );
        worker.setField( f, array );
        assertEquals( expected, worker.getArrayField( String.class, f ) );

        worker.setField( f, "[\"a\",\"b\",\"c\"]" );
        assertEquals( expected, worker.getArrayField( String.class, f ) );

        // integer array
        List<Integer> expectedInt = Arrays.asList( new Integer[] { 1, 2, 3 } );

        array.clear();
        array.addAll( expectedInt );
        worker.setField( f, array );
        List<Integer> actual = worker.getArrayField( Integer.class, f );
        assertEquals( expectedInt, actual );

        worker.setField( f, "[1,2,3]" );
        actual = worker.getArrayField( Integer.class, f );
        assertArrayEquals( expectedInt.toArray(), actual.toArray() );
    }

}
