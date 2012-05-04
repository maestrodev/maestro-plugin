package com.maestrodev;

import static org.fusesource.stomp.client.Constants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.fusesource.stomp.client.BlockingConnection;
import org.fusesource.stomp.client.Stomp;
import org.fusesource.stomp.codec.StompFrame;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for MaestroWorker.
 */
public class MaestroWorkerTest 
{
    
    BrokerService broker = new BrokerService();
    
    @Before
    public void setUp() throws Exception {
        // configure the broker
        Thread thread = new Thread () {
            public void run(){
            TransportConnector connector = new TransportConnector();
                try {
                    connector.setUri(new URI("stomp://localhost:61619"));
                    broker.addConnector(connector);
                    broker.setBrokerName("test_broker");
                    broker.start();
                } catch (Exception ex) {
                    throw new RuntimeException( "Unable To Create Broker", ex );
                }
            
            }
        };
        thread.start();
        
        Thread.sleep(10000);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
        Thread.sleep(1000);
    }

    @Test
    public void testWriteOutput() throws IOException, URISyntaxException, ParseException
    {

        HashMap config = new HashMap();
        config.put("host", "localhost");
        config.put("port", "61619");
        config.put("queue", "/queue/test");

        JSONObject workitem = new JSONObject();

        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        worker.setStompConfig(config);


        Stomp stomp = new Stomp("localhost", 61619);
        BlockingConnection connection = stomp.connectBlocking();


        StompFrame frame = new StompFrame(SUBSCRIBE);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
        frame.addHeader(ID, connection.nextId());
        StompFrame response = connection.request(frame);

        // This unblocks once the response frame is received.
        assertNotNull(response);

        worker.writeOutput("Hello Maestro Plugin!");

        // Try to get the received message.
        StompFrame received = connection.receive();
        assertTrue(received.action().equals(MESSAGE));
        JSONParser parser = new JSONParser();
        
        workitem = (JSONObject) parser.parse(received.content().ascii().toString());
        assertTrue(workitem.get("__output__").equals("Hello Maestro Plugin!"));
       
    }
    
    @Test
    public void testSetField() throws IOException, URISyntaxException, ParseException
    {

        

        JSONObject workitem = new JSONObject();
        JSONObject fields = new JSONObject();
        workitem.put("fields", fields);
        
        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        
        worker.setField("some field", "some value");
       
        assertEquals(worker.getField("some field"), "some value");
    }
    
    @Test
    public void testCancel() throws IOException, URISyntaxException, ParseException
    {

        HashMap config = new HashMap();
        config.put("host", "localhost");
        config.put("port", "61619");
        config.put("queue", "/queue/test");

        JSONObject workitem = new JSONObject();

        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        worker.setStompConfig(config);


        Stomp stomp = new Stomp("localhost", 61619);
        BlockingConnection connection = stomp.connectBlocking();


        StompFrame frame = new StompFrame(SUBSCRIBE);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
        frame.addHeader(ID, connection.nextId());
        StompFrame response = connection.request(frame);

        // This unblocks once the response frame is received.
        assertNotNull(response);

        worker.cancel();

        // Try to get the received message.
        StompFrame received = connection.receive();
        assertTrue(received.action().equals(MESSAGE));
        JSONParser parser = new JSONParser();
        
        workitem = (JSONObject) parser.parse(received.content().ascii().toString());
        assertTrue(workitem.get("__cancel__").equals("true"));
       
    }

    @Test
    public void testSetWaiting() throws IOException, URISyntaxException, ParseException
    {

        HashMap config = new HashMap();
        config.put("host", "localhost");
        config.put("port", "61619");
        config.put("queue", "/queue/test");

        JSONObject workitem = new JSONObject();

        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        worker.setStompConfig(config);


        Stomp stomp = new Stomp("localhost", 61619);
        BlockingConnection connection = stomp.connectBlocking();


        StompFrame frame = new StompFrame(SUBSCRIBE);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
        frame.addHeader(ID, connection.nextId());
        StompFrame response = connection.request(frame);

        // This unblocks once the response frame is received.
        assertNotNull(response);

        worker.setWaiting(true);

        // Try to get the received message.
        StompFrame received = connection.receive();
        assertTrue(received.action().equals(MESSAGE));
        JSONParser parser = new JSONParser();
        
        workitem = (JSONObject) parser.parse(received.content().ascii().toString());
        assertTrue(workitem.get("__waiting__").equals("true"));
    }

    @Test
    public void testUpdateFieldsInRecord() throws IOException, URISyntaxException, ParseException
    {

        HashMap config = new HashMap();
        config.put("host", "localhost");
        config.put("port", "61619");
        config.put("queue", "/queue/test");

        JSONObject workitem = new JSONObject();

        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        worker.setStompConfig(config);


        Stomp stomp = new Stomp("localhost", 61619);
        BlockingConnection connection = stomp.connectBlocking();


        StompFrame frame = new StompFrame(SUBSCRIBE);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
        frame.addHeader(ID, connection.nextId());
        StompFrame response = connection.request(frame);

        // This unblocks once the response frame is received.
        assertNotNull(response);

        worker.updateFieldsInRecord("model", "name or id", "field", "value");

        // Try to get the received message.
        StompFrame received = connection.receive();
        assertTrue(received.action().equals(MESSAGE));
        JSONParser parser = new JSONParser();
        
        workitem = (JSONObject) parser.parse(received.content().ascii().toString());
        assertTrue(workitem.get("__persist__").equals("true"));
        assertTrue(workitem.get("__update__").equals("true"));
        assertTrue(workitem.get("__model__").equals("model"));
        assertTrue(workitem.get("__record_id__").equals("name or id"));
        assertTrue(workitem.get("__record_field__").equals("field"));
        assertTrue(workitem.get("__record_value__").equals("value"));
    }
    
    @Test
    public void testCreateRecordWithFields() throws IOException, URISyntaxException, ParseException
    {

        HashMap config = new HashMap();
        config.put("host", "localhost");
        config.put("port", "61619");
        config.put("queue", "/queue/test");

        JSONObject workitem = new JSONObject();

        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        worker.setStompConfig(config);


        Stomp stomp = new Stomp("localhost", 61619);
        BlockingConnection connection = stomp.connectBlocking();


        StompFrame frame = new StompFrame(SUBSCRIBE);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
        frame.addHeader(ID, connection.nextId());
        StompFrame response = connection.request(frame);

        // This unblocks once the response frame is received.
        assertNotNull(response);

        String [] fields = {"field"};
        String [] values = {"value"};
        worker.createRecordWithFields("model", fields, values);

        // Try to get the received message.
        StompFrame received = connection.receive();
        assertTrue(received.action().equals(MESSAGE));
        JSONParser parser = new JSONParser();
        
        workitem = (JSONObject) parser.parse(received.content().ascii().toString());
        assertTrue(workitem.get("__persist__").equals("true"));
        assertTrue(workitem.get("__create__").equals("true"));        
        assertTrue(workitem.get("__model__").equals("model"));
        assertTrue(workitem.get("__record_fields__").equals("field"));
        assertTrue(workitem.get("__record_values__").equals("value"));
    }
    
    @Test
    public void testDeleteRecord() throws IOException, URISyntaxException, ParseException
    {

        HashMap config = new HashMap();
        config.put("host", "localhost");
        config.put("port", "61619");
        config.put("queue", "/queue/test");

        JSONObject workitem = new JSONObject();

        MaestroWorker worker = new MaestroWorker();
        worker.setWorkitem(workitem);
        worker.setStompConfig(config);


        Stomp stomp = new Stomp("localhost", 61619);
        BlockingConnection connection = stomp.connectBlocking();


        StompFrame frame = new StompFrame(SUBSCRIBE);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
        frame.addHeader(ID, connection.nextId());
        StompFrame response = connection.request(frame);

        // This unblocks once the response frame is received.
        assertNotNull(response);

        
        worker.deleteRecord("model", "name_or_id");

        // Try to get the received message.
        StompFrame received = connection.receive();
        assertTrue(received.action().equals(MESSAGE));
        JSONParser parser = new JSONParser();
        
        workitem = (JSONObject) parser.parse(received.content().ascii().toString());
        assertTrue(workitem.get("__persist__").equals("true"));
        assertTrue(workitem.get("__delete__").equals("true"));        
        assertTrue(workitem.get("__model__").equals("model"));
        assertTrue(workitem.get("__name__").equals("name_or_id"));
    }
}
