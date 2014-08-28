/*
 * Copyright (c) 2013, MaestroDev. All rights reserved.
 */
package com.maestrodev.maestro.plugins;

import static org.fusesource.stomp.client.Constants.*;
import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.fusesource.stomp.client.BlockingConnection;
import org.fusesource.stomp.client.Stomp;
import org.fusesource.stomp.codec.StompFrame;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.jr.ob.JSON;
import com.maestrodev.maestro.plugins.MaestroWorker;

/**
 * Test MaestroWorker with a Stomp broker.
 */
public class MaestroWorkerStompTest {

    private static final String HOST = "localhost";

    private static final int PORT = 61619;

    private BrokerService broker = new BrokerService();
    
    HashMap<String,Object> config;

    private Map<String, Object> workitem;

    @Before
    public void setUp() throws Exception {
        workitem = new HashMap<String, Object>();
	config = new HashMap<String,Object>();
	config.put("host", "localhost");
	config.put("port", "61619");
	config.put("queue", "/queue/test");
	
	// configure the broker
	TransportConnector connector = new TransportConnector();
	connector.setUri(new URI("stomp://" + HOST + ":" + PORT));
	broker.addConnector(connector);
	broker.setPersistent(false);
	broker.setBrokerName("test_broker");
	broker.start();
    }

    @After
    public void tearDown() throws Exception {
	if (broker != null) {
	    broker.stop();
	}
    }

    public Stomp getStomp() {
	try {
	    return new Stomp(HOST, PORT);
	} catch (URISyntaxException e) {
	    throw new RuntimeException(e);
	}
    }

    @Test
    public void testWriteOutput() throws Exception {

	MaestroWorker worker = new MaestroWorker();
	worker.setWorkitem(workitem);
	worker.setStompConfig(config);

	Stomp stomp = getStomp();
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

	workitem = JSON.std.mapFrom(received.content().ascii().toString());
	assertTrue(workitem.get("__output__").equals("Hello Maestro Plugin!"));

    }

    @Test
    public void testNotNeeded() throws Exception {

	MaestroWorker worker = new MaestroWorker();
	worker.setWorkitem(workitem);
	worker.setStompConfig(config);

	Stomp stomp = getStomp();
	BlockingConnection connection = stomp.connectBlocking();

	StompFrame frame = new StompFrame(SUBSCRIBE);
	frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
	frame.addHeader(ID, connection.nextId());
	StompFrame response = connection.request(frame);

	// This unblocks once the response frame is received.
	assertNotNull(response);

	worker.notNeeded();

	// Try to get the received message.
	StompFrame received = connection.receive();
	assertTrue(received.action().equals(MESSAGE));

	workitem = JSON.std.mapFrom(received.content().ascii().toString());
	assertTrue(workitem.get("__not_needed__").equals("true"));

    }

    @Test
    public void testCancel() throws Exception {

	MaestroWorker worker = new MaestroWorker();
	worker.setWorkitem(workitem);
	worker.setStompConfig(config);

	Stomp stomp = getStomp();
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

    workitem = JSON.std.mapFrom(received.content().ascii().toString());
	assertTrue(workitem.get("__cancel__").equals("true"));

    }

    @Test
    public void testSetWaiting() throws Exception {

	MaestroWorker worker = new MaestroWorker();
	worker.setWorkitem(workitem);
	worker.setStompConfig(config);

	Stomp stomp = getStomp();
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

    workitem = JSON.std.mapFrom(received.content().ascii().toString());
	assertTrue(workitem.get("__waiting__").equals("true"));
    }

    @Test
    public void testUpdateFieldsInRecord() throws Exception {

	MaestroWorker worker = new MaestroWorker();
	worker.setWorkitem(workitem);
	worker.setStompConfig(config);

	Stomp stomp = getStomp();
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

    workitem = JSON.std.mapFrom(received.content().ascii().toString());
	assertTrue(workitem.get("__persist__").equals("true"));
	assertTrue(workitem.get("__update__").equals("true"));
	assertTrue(workitem.get("__model__").equals("model"));
	assertTrue(workitem.get("__record_id__").equals("name or id"));
	assertTrue(workitem.get("__record_field__").equals("field"));
	assertTrue(workitem.get("__record_value__").equals("value"));
    }

    @Test
    public void testCreateRecordWithFields() throws Exception {
	MaestroWorker worker = new MaestroWorker();
	worker.setWorkitem(workitem);
	worker.setStompConfig(config);

	Stomp stomp = getStomp();
	BlockingConnection connection = stomp.connectBlocking();

	StompFrame frame = new StompFrame(SUBSCRIBE);
	frame.addHeader(DESTINATION, StompFrame.encodeHeader("/queue/test"));
	frame.addHeader(ID, connection.nextId());
	StompFrame response = connection.request(frame);

	// This unblocks once the response frame is received.
	assertNotNull(response);

	String[] fields = { "field" };
	String[] values = { "value" };
	worker.createRecordWithFields("model", fields, values);

	// Try to get the received message.
	StompFrame received = connection.receive();
	assertTrue(received.action().equals(MESSAGE));

    workitem = JSON.std.mapFrom(received.content().ascii().toString());
	assertTrue(workitem.get("__persist__").equals("true"));
	assertTrue(workitem.get("__create__").equals("true"));
	assertTrue(workitem.get("__model__").equals("model"));
	assertTrue(workitem.get("__record_fields__").equals("field"));
	assertTrue(workitem.get("__record_values__").equals("value"));
    }

    @Test
    public void testDeleteRecord() throws Exception {
	MaestroWorker worker = new MaestroWorker();
	worker.setWorkitem(workitem);
	worker.setStompConfig(config);

	Stomp stomp = getStomp();
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

    workitem = JSON.std.mapFrom(received.content().ascii().toString());
	assertTrue(workitem.get("__persist__").equals("true"));
	assertTrue(workitem.get("__delete__").equals("true"));
	assertTrue(workitem.get("__model__").equals("model"));
	assertTrue(workitem.get("__name__").equals("name_or_id"));
    }
}
