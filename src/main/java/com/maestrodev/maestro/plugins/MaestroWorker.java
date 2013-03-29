/*
 * Copyright (c) 2013, MaestroDev. All rights reserved.
 */
package com.maestrodev.maestro.plugins;

import static java.lang.String.*;
import static org.apache.commons.lang3.exception.ExceptionUtils.*;
import static org.fusesource.stomp.client.Constants.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.stomp.client.BlockingConnection;
import org.fusesource.stomp.codec.StompFrame;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.json.simple.JSONArray;

/**
 * Helper Class for Maestro Plugins written in Java. The lifecycle of the plugin
 * starts with a call to {@link #setStompConfig(Map)} and then the main entry
 * point is {@link #perform(String, Map)}, called by the Maestro agent, all the
 * other methods are helpers that can be used to deal with parsing, errors, etc.
 */
public class MaestroWorker {
    
    private static final Logger logger = Logger.getLogger(MaestroWorker.class.getName());
    
    private static final String CREATE_META = "__create__";
    private static final String DELETE_META = "__delete__";
    private static final String NAME_META = "__name__";
    private static final String RECORD_VALUES_META = "__record_values__";
    private static final String RECORD_FIELDS_META = "__record_fields__";
    private static final String RECORD_VALUE_META = "__record_value__";
    private static final String RECORD_FIELD_META = "__record_field__";
    private static final String RECORD_ID_META = "__record_id__";
    private static final String MODEL_META = "__model__";
    private static final String UPDATE_META = "__update__";
    private static final String PERSIST_META = "__persist__";
    private static final String STREAMING_META = "__streaming__";
    private static final String OUTPUT_META = "__output__";
    private static final String WAITING_META = "__waiting__";
    private static final String CANCEL_META = "__cancel__";
    private static final String NOT_NEEDED_META = "__not_needed__";
    private static final String LINKS_META = "__links__";

    private JSONObject workitem;
    private Map<String, Object> stompConfig = new HashMap<String, Object>();
    private StompConnectionFactory stompConnectionFactory;

    /**
     * Creates a new instance with the default StompConnectionFactory.
     */
    protected MaestroWorker() {
	this(StompConnectionFactory.getInstance());
    }
    
    /**
     * Creates a new instance with the specified StompConnectionFactory.
     * 
     * @param stompConnectionFactory a StompConnectionFactory
     */
    protected MaestroWorker(StompConnectionFactory stompConnectionFactory) {
	setStompConnectionFactory(stompConnectionFactory);
    }

    /**
     * Sends back the "not needed" message that stops composition execution.
     * 
     */
    public void notNeeded() {
	try {
	    String[] fields = { NOT_NEEDED_META };
	    String[] values = { String.valueOf(true) };
	    sendFieldsWithValues(fields, values);
	} catch (Exception e) {
	    logger.log(Level.SEVERE, "Error sending cancel message", e);
	}
    }

    /**
     * Sends the "cancel" message that stops composition execution.
     * 
     */
    public void cancel() {
	try {
	    String[] fields = { CANCEL_META };
	    String[] values = { String.valueOf(true) };
	    sendFieldsWithValues(fields, values);
	} catch (Exception e) {
	    logger.log(Level.SEVERE, "Error sending cancel message", e);
	}
    }

    /**
     * Puts the current task run in a waiting state or allows it to continue
     * 
     * @param waiting set to true to put in a waiting state.
     */
    public void setWaiting(boolean waiting) {
	try {
	    String[] fields = { WAITING_META };
	    String[] values = { String.valueOf(waiting) };
	    sendFieldsWithValues(fields, values);
	} catch (Exception e) {
	    logger.log(Level.SEVERE, "Error setting waiting to " + waiting, e);
	}
    }

    /**
     * Sends the specified output strings to the server for persistence.
     * 
     * @param output the message to be persisted.
     */
    public void writeOutput(String output) {
	try {
	    String[] fields = { OUTPUT_META, STREAMING_META };
	    String[] values = { output, String.valueOf(true) };
	    sendFieldsWithValues(fields, values);
	} catch (Exception e) {
	    logger.log(Level.SEVERE, "Error writing output: " + output, e);
	}
    }

    /**
     * Sends the specified field/value pairs.
     * 
     * @param fields the fields.
     * @param values the values.
     */
    @SuppressWarnings("unchecked")
    private void sendFieldsWithValues(String[] fields, String[] values) {
	if (fields.length != values.length) {
	    throw new IllegalArgumentException(
		    "Mismatched Field and Value Sets fields.length != values.length");
	}
	if (this.workitem == null) {
	    throw new IllegalStateException("Workitem has not been set yet");
	}

	for (int i = 0; i < fields.length; i++) {
	    this.workitem.put(fields[i], values[i]);
	}

	BlockingConnection connection = null;

	try {
	    connection = this.getConnection();
	    this.sendCurrentWorkitem(connection);
	} catch (IOException e) {
	    throw new RuntimeException("Error connecting to Stomp server", e);
	} catch (URISyntaxException e) {
	    throw new RuntimeException("Error connecting to Stomp server", e);
	} finally {
	    closeConnectionAndCleanup(connection, fields);
	}
    }

    /**
     * Sends the current work item data to the server.
     * 
     * @param connection
     * @throws IOException
     */
    private void sendCurrentWorkitem(BlockingConnection connection)
	    throws IOException {

	Object queue = this.stompConfig.get("queue");
	if (queue == null) {
	    logger.log(Level.SEVERE,
		    "Missing Stomp Configuration. Make Sure Queue is Set");
	    return;
	}

	StompFrame frame = new StompFrame(SEND);
	frame.addHeader(DESTINATION, StompFrame.encodeHeader(queue.toString()));
	Buffer buffer = new Buffer(this.workitem.toJSONString().getBytes());
	frame.content(buffer);

	connection.send(frame);

	try {
	    Thread.sleep(500);
	} catch (InterruptedException ex) {
	    logger.log(Level.SEVERE, "Sleep interrupted", ex);
	}

    }

    /**
     * Gets a new stomp connection.
     * 
     * @return a stomp connection
     * @throws IOException if the connection could not be established.
     * @throws URISyntaxException if the host or port is missing from the
     *                            stomp configuration.
     */
    private BlockingConnection getConnection() throws IOException,
	    URISyntaxException {

	Object h = this.stompConfig.get("host");
	Object p = this.stompConfig.get("port");

	if ((h == null) || (p == null)) {
	    throw new IllegalStateException(
		    "Missing Stomp Configuration. Make Sure Host and Port Are Set");
	}

	return stompConnectionFactory.getConnection(h.toString(),
		Integer.parseInt(p.toString()));

    }
    
    /**
     * Closes the specified connection and cleans up the state of the worker.
     * 
     * @param connection the connection to close.
     */
    private void closeConnectionAndCleanup(BlockingConnection connection) {
	if (connection != null) {
	    connection.suspend();
	    try {
		connection.close();
	    } catch (IOException e) {
		// ignore
	    }
	}

	this.workitem.remove(OUTPUT_META);
	this.workitem.remove(STREAMING_META);
	this.workitem.remove(CANCEL_META);
	if (this.workitem.get(WAITING_META) != null
		&& !Boolean.parseBoolean(this.workitem.get(WAITING_META).toString())) {
	    this.workitem.remove(WAITING_META);
	}
    }
    
    /**
     * Closes the stomp connection and cleans up.
     * 
     * @param connection the stomp connection.
     * @param fields the work item fields.
     */
    private void closeConnectionAndCleanup(BlockingConnection connection,
	    String[] fields) {
	this.closeConnectionAndCleanup(connection);
	for (String field : fields) {
	    if (!WAITING_META.equals(field))
		this.workitem.remove(field);
	}
    }

    /**
     * Sets the error field in the work item.
     * 
     * @param error Error message
     */
    @SuppressWarnings("unchecked")
    public void setError(String error) {
	getFields().put("__error__", error);
    }

    /**
     * Returns the error field from the work item.
     */
    public String getError() {
	return getField("__error__");
    }

    /**
     * Retrieve the specified field from the work item.
     * 
     * @param field key to get value for
     * @return field value
     */
    public String getField(String field) {
	if (getFields().get(field) == null) {
	    return null;
	}
	return getFields().get(field).toString();
    }

    /**
     * Returns the specified array field from the work item.
     * 
     * Caveat: lists of integers are returned as lists of longs
     * 
     * @param clazz the type of object contained in the field's array.
     * @param field key to get value for
     * @return a list containing the field's values.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getArrayField(Class<T> clazz, String field) {
	JSONObject fields = getFields();
	Object o = fields.get(field);
	if (o == null) {
	    return null;
	}
	if (o instanceof List) {
	    return (List<T>) o;
	}
	throw new IllegalArgumentException(format(
		"Field %s is not an array nor can be parsed as such: %s",
		field, o));
    }

    /**
     * Main entry point from the agent.
     * 
     * @param methodName name of the method to execute.
     * @param workitem the work item.
     * @return the work item
     */
    @SuppressWarnings("rawtypes")
    public final Map perform(String methodName, Map workitem) {
	String className = this.getClass().getName();
	try {
	    JSONParser parser = new JSONParser();
	    String json = JSONObject.toJSONString(workitem);
	    setWorkitem((JSONObject) parser.parse(json));

	    writeOutput(format("Executing plugin: %s.%s%n", className, methodName));

	    Method method = getClass().getMethod(methodName);
	    method.invoke(this);

	    writeOutput(format("Finished plugin execution: %s.%s%n", className,
		    methodName));

	} catch (InvocationTargetException e) {
	    // get the root cause of the exception
	    String msg = format("Plugin %s.%s failed: %s ", className, methodName,
		    getStackTrace(e.getCause()));
	    this.writeOutput(msg);
	    this.setError(msg);
	} catch (Exception e) {
	    String msg = format("Plugin %s.%s failed: %s ", className, methodName,
		    getStackTrace(e));
	    this.writeOutput(msg);
	    this.setError(msg);
	}
	return getWorkitem();
    }

    /**
     * Returns the fields from the work item.
     * 
     * @return the work item fields.
     */
    public JSONObject getFields() {
	return ((JSONObject) getWorkitem().get("fields"));
    }

    /**
     * Sets the specified field to the specified value in the work item.
     * 
     * @param name the field name
     * @param value value to apply to field
     */
    @SuppressWarnings("unchecked")
    public void setField(String name, Object value) {
	getFields().put(name, value);
    }

    /**
     * Adds a link to be displayed in the Maestro UI.
     * 
     * @param name the name of the link.
     * @param url the link URL.
     * 
     */
    @SuppressWarnings("unchecked")
    public void addLink(String name, String url) {
	JSONObject fields = getFields();
	JSONArray links = (JSONArray) fields.get(LINKS_META);
	if (links == null) {
	    links = new JSONArray();
	    fields.put(LINKS_META, links);
	}

	JSONObject link = new JSONObject();
	link.put("name", name);
	link.put("url", url);
	links.add(link);
    }

    /**
     * Getter for accessing the work item
     * 
     * @return the work item.
     */
    public JSONObject getWorkitem() {
	return workitem;
    }

    /**
     * Sets the work item.
     * 
     * @param workitem the work item.
     */
    public void setWorkitem(JSONObject workitem) {
	this.workitem = workitem;
    }

    /**
     * Returns the stomp configuration.
     * 
     * @return
     */
    public Map<String, Object> getStompConfig() {
	return stompConfig;
    }

    /**
     * Sets the stomp configuration.
     * 
     * @param stompConfig
     */
    public void setStompConfig(Map<String, Object> stompConfig) {
	this.stompConfig = stompConfig;
    }

    
    /**
     * Sets the StompConnectionFactory. This is used to help during unit testing.
     */
    public void setStompConnectionFactory(StompConnectionFactory stompConnectionFactory) {
	this.stompConnectionFactory = stompConnectionFactory;
    }
    
    /**
     * Updates the record in the database with the specified field value.
     * @param model the table or model name.
     * @param nameOrId the unique name or ID.
     * @param field the field name.
     * @param value the field value.
     */
    void updateFieldsInRecord(String model, String nameOrId,
	    String field, String value) {
	try {

	    String[] fields = { PERSIST_META, UPDATE_META, MODEL_META,
		    RECORD_ID_META, RECORD_FIELD_META, RECORD_VALUE_META };
	    String[] values = { String.valueOf(true), String.valueOf(true),
		    model, nameOrId, field, value };
	    sendFieldsWithValues(fields, values);
	} catch (Exception e) {
	    logger.log(Level.SEVERE, "Error updating fields in record, field: "
		    + field + ", value: " + value, e);
	}
    }

    /**
     * Creates a new database record.
     * @param model the name of the table/model.
     * @param recordFields the field names.
     * @param recordValues the field values.
     */
    void createRecordWithFields(String model, String[] recordFields,
	    String[] recordValues) {
	try {

	    String[] fields = { PERSIST_META, CREATE_META, MODEL_META,
		    RECORD_FIELDS_META, RECORD_VALUES_META };
	    String[] values = { String.valueOf(true), String.valueOf(true),
		    model, StringUtils.join(recordFields, ","),
		    StringUtils.join(recordValues, ",") };
	    sendFieldsWithValues(fields, values);
	} catch (Exception e) {
	    logger.log(Level.SEVERE, "Error creating record, fields: "
		    + StringUtils.join(recordFields, ",") + ", values: "
		    + StringUtils.join(recordValues, ","), e);
	}
    }
    
    /**
     * Deletes a record from the database.
     * @param model the name of the table/model.
     * @param nameOrId the record unique name or ID.
     */
    void deleteRecord(String model, String nameOrId) {
	try {
	    String[] fields = { PERSIST_META, DELETE_META, MODEL_META,
		    NAME_META };
	    String[] values = { String.valueOf(true), String.valueOf(true),
		    model, nameOrId };
	    sendFieldsWithValues(fields, values);
	} catch (Exception e) {
	    logger.log(Level.SEVERE, "Error deleting record: " + model + " - "
		    + nameOrId, e);
	}
    }

}
