package com.maestrodev;

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
 * Helper Class for Maestro Plugins written in Java. The lifecycle of the plugin starts with a call to
 * {@link #setStompConfig(Map)} and then the main entry point is {@link #perform(String, Map)}, called by the Maestro
 * agent, all the other methods are helpers that can be used to deal with parsing, errors,...
 */
public class MaestroWorker 
{
    private static Logger logger = Logger.getLogger(MaestroWorker.class.getName());

    
    private JSONObject workitem;
    private Map<String, Object> stompConfig = new HashMap<String, Object>();
    private StompConnectionFactory stompConnectionFactory;

    
    
    protected MaestroWorker()
    {
        this.stompConnectionFactory = StompConnectionFactory.getInstance();        
    }

    
    
    public MaestroWorker(StompConnectionFactory stompConnectionFactory)
    {
        super();
        this.stompConnectionFactory = stompConnectionFactory;
    }



    /**
     * Helper that sends cancel message that stops composition execution.
     * 
     */
     public void cancel() {
        try{
            
            String [] fields = {"__cancel__"};
            String [] values = {String.valueOf(true)};
            sendFieldsWithValues(fields, values);
        }catch(Exception e){
            logger.log(Level.SEVERE, "Error sending cancel message", e);
        }
    }
     
     /**
     * Puts the current task run in a waiting state or allows it to continue
     * 
     * @param waiting - Will I wait or won't I?
     */
    public void setWaiting(boolean waiting) {
        try{
            
            String [] fields = {"__waiting__"};
            String [] values = {String.valueOf(waiting)};
            sendFieldsWithValues(fields, values);
        }catch(Exception e){
            logger.log(Level.SEVERE, "Error setting waiting to " + waiting, e);
        }
    }
    
    
    /**
     * Helper that sends output strings to server for persistence.
     * 
     * @param output - Message to be persisted for the associated TaskExecution
     */
    public void writeOutput(String output){
        try{
            
            String [] fields = {"__output__","__streaming__"};
            String [] values = {output, String.valueOf(true)};
            sendFieldsWithValues(fields, values);

        }catch(Exception e){
            logger.log(Level.SEVERE, "Error writing output: " + output, e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void sendFieldsWithValues(String [] fields, String [] values) {
        if(fields.length != values.length){
            throw new IllegalArgumentException("Mismatched Field and Value Sets fields.length != values.length" );
        }
        if (this.workitem == null) {
            throw new IllegalStateException("Workitem has not been set yet");
        }
        
        
        for(int ii = 0 ; ii < fields.length ; ++ii){
            this.workitem.put(fields[ii], values[ii]);
        }
        
        BlockingConnection connection = null;
        
        try {
	    connection = this.getConnection();

	    this.sendCurrentWorkitem(connection);
        } catch (IOException e) {
	    throw new RuntimeException( "Error connecting to Stomp server", e );
        } catch (URISyntaxException e) {
            throw new RuntimeException( "Error connecting to Stomp server", e );
        } finally {
            closeConnectionAndCleanup(connection, fields);
        }
    }
    
    private void sendCurrentWorkitem(BlockingConnection connection) throws IOException{

        Object queue = this.stompConfig.get("queue");
        if ( queue == null )
        {
            logger.log( Level.SEVERE, "Missing Stomp Configuration. Make Sure Queue is Set" );
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
    
    private BlockingConnection getConnection()throws IOException, URISyntaxException{

        Object h = this.stompConfig.get( "host" );
        Object p = this.stompConfig.get( "port" );

        if ( ( h == null ) || ( p == null ) )
        {
            throw new IllegalStateException("Missing Stomp Configuration. Make Sure Host and Port Are Set");
        }

        return stompConnectionFactory.getConnection(h.toString(), Integer.parseInt( p.toString() ));
        
    }

    private void closeConnectionAndCleanup(BlockingConnection connection) {
	if (connection != null) {
	    connection.suspend();
	    try {
		connection.close();
	    } catch (IOException e) {
		// ignore
	    }
	}
        
        this.workitem.remove("__output__");
        this.workitem.remove("__streaming__");
        this.workitem.remove("__cancel__");
        if(this.workitem.get("__waiting__") != null && Boolean.getBoolean(
            this.workitem.get("__waiting__").toString()) == false){
            this.workitem.remove("__waiting__");
        }
    }
    
    /**
     * Helper method for setting the error field
     * 
     * @param error - Error message
     */
    @SuppressWarnings("unchecked")
    public void setError(String error){
        getFields().put("__error__", error);
    }
    
    /**
     * Helper method for getting the error field
     */
    public String getError(){
        return getField( "__error__" );
    }

    /**
     * Helper method for getting a string field
     * 
     * @param field key to get value for
     * @return field value
     */
    public String getField(String field){
        if(getFields().get(field) == null){
            return null;
        }
        return getFields().get(field).toString();
    }
    
    /**
     * Helper method for getting an array field.
     * 
     * Caveat: lists of integers are returned as lists of longs
     *
     * @param field key to get value for
     * @return field value
     */
    @SuppressWarnings( "unchecked" )
    public <T> List<T> getArrayField( Class<T> clazz, String field )
    {
        JSONObject fields = getFields();
        Object o = fields.get( field );
        if ( o == null )
        {
            return null;
        }
        if ( o instanceof List )
        {
            return (List<T>) o;
        }
        throw new IllegalArgumentException( format( "Field %s is not an array nor can be parsed as such: %s", field, o ) );
    }

    /**
     * Main entry point from the agent.
     * 
     * @param methodName name of the method to execute
     * @param workitem JSON configuration
     * @return
     */    
    @SuppressWarnings("rawtypes")
    public Map perform(String methodName, Map workitem) {
        String clazz = this.getClass().getName();
        try{
            JSONParser parser = new JSONParser();
            String json = JSONObject.toJSONString(workitem);
            setWorkitem((JSONObject)parser.parse(json));

            writeOutput(format("Executing plugin: %s.%s%n", clazz, methodName));

            Method method = getClass().getMethod(methodName);
            method.invoke(this);

            writeOutput(format("Finished plugin execution: %s.%s%n", clazz, methodName));
        
        } catch (InvocationTargetException e) {
            // get the root cause of the exception
            String msg = format("Plugin %s.%s failed: %s ", clazz, methodName, getStackTrace( e.getCause() ));
            this.writeOutput(msg);
            this.setError(msg);
        } catch (Exception e) {
            String msg = format("Plugin %s.%s failed: %s ", clazz, methodName, getStackTrace( e ));
            this.writeOutput(msg);
            this.setError(msg);
        }
        return getWorkitem();
    }

    /**
     * Helper method for getting the fields
     * 
     * @return fields set
     */
    public JSONObject getFields(){
        return ((JSONObject)getWorkitem().get("fields"));
    }
    
    /**
     * Helper method for setting fields
     * @param name string key field name
     * @param value value to apply to field
     * 
     */
    @SuppressWarnings( "unchecked" )
    public void setField(String name, Object value){
        getFields().put(name, value);
    }
    
     /**
     * Helper method for adding links to the UI
     * @param name string url name
     * @param url string url to link to
     * 
     */
    @SuppressWarnings("unchecked")
    public void addLink(String name, String url){
        if(((JSONObject)getWorkitem().get("fields")).get("__links__") == null) {
          ((JSONObject)getWorkitem().get("fields")).put("__links__", new JSONArray());
        }
        
        JSONArray links = (JSONArray) ((JSONObject)getWorkitem().get("fields")).get("__links__");
        JSONObject link = new JSONObject();
        link.put("name", name);
        link.put("url", url);
        links.add(link);
    }
    
    /**
     * getter for accessing the Workitem
     * 
     * @return Map of Workitem values
     */
    public JSONObject getWorkitem() {
        return workitem;
    }

    /**
     * setter for overwriting all of the Workitem values
     * 
     * @param workitem 
     */
    public void setWorkitem(JSONObject workitem) {
        this.workitem = workitem;
    }
    
    public Map<String, Object> getStompConfig() {
        return stompConfig;
    }

    public void setStompConfig(Map<String, Object> stompConfig) {
        this.stompConfig = stompConfig;
    }

    
    /*
     * Database 
     */
    
    public void updateFieldsInRecord(String model, String nameOrId, String field, String value) {
        try{
            
            String [] fields = {"__persist__", "__update__", "__model__", "__record_id__", "__record_field__", "__record_value__"};
            String [] values = {String.valueOf(true), String.valueOf(true), model, nameOrId, field, value};
            sendFieldsWithValues(fields, values);
        }catch(Exception e){
            logger.log(Level.SEVERE, "Error updating fields in record, field: " + field + ", value: " + value, e);
        }
    }

    private void closeConnectionAndCleanup(BlockingConnection connection, String [] fields) {
        this.closeConnectionAndCleanup(connection);
        for (String field : fields) {
            if (!"__waiting__".equals(field))
                this.workitem.remove(field);
        }
    }

    void createRecordWithFields(String model, String[] recordFields, String[] recordValues) {
         try{
            
            String [] fields = {"__persist__", "__create__", "__model__", "__record_fields__", "__record_values__"};
            String [] values = {String.valueOf(true), String.valueOf(true), model, StringUtils.join(recordFields, ","), StringUtils.join(recordValues, ",")};
            sendFieldsWithValues(fields, values);
        }catch(Exception e){
            logger.log( Level.SEVERE, "Error creating record, fields: " + StringUtils.join( recordFields, "," )
                + ", values: " + StringUtils.join( recordValues, "," ), e );
        }
    }

    void deleteRecord(String model, String nameOrId) {
        try{
            
            String [] fields = {"__persist__", "__delete__", "__model__", "__name__"};
            String [] values = {String.valueOf(true), String.valueOf(true), model, nameOrId};
            sendFieldsWithValues(fields, values);
        }catch(Exception e){
            logger.log(Level.SEVERE, "Error deleting record: " + model + " - " + nameOrId, e);
        }
    }

   /*
    * End Database 
    */
}
