package com.maestrodev;

import java.util.Map;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.fusesource.hawtbuf.Buffer;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.fusesource.stomp.client.BlockingConnection;
import org.fusesource.stomp.client.Stomp;
import org.fusesource.stomp.codec.StompFrame;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import static org.fusesource.stomp.client.Constants.*;
import org.json.simple.parser.ParseException;

/**
 * Main Class for Maestro Plugins written in Java.
 *
 */
public class MaestroWorker 
{
    private JSONObject workitem;
    private Map stompConfig;


    
    public MaestroWorker(){
        this.workitem = null;
        this.stompConfig = null;
    }
    
    /**
     * Helper that sends cancel message that stops composition execution.
     * 
     */
     public void cancel() {
        try{
            
            String [] fields = {"__cancel__"};
            String [] values = {String.valueOf(true)};
            BlockingConnection connection = sendFieldsWithValues(fields, values);

            closeConnectionAndCleanup(connection);
        }catch(NullPointerException e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, 
                    "Missing Stomp Configuration,"+
                    " Make Sure Please Make Sure Host, Port And Queue Are Set");
        }catch(Exception e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, e);
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
            BlockingConnection connection = sendFieldsWithValues(fields, values);

            closeConnectionAndCleanup(connection);
        }catch(NullPointerException e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, 
                    "Missing Stomp Configuration,"+
                    " Make Sure Please Make Sure Host, Port And Queue Are Set");
        }catch(Exception e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, e);
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
            BlockingConnection connection = sendFieldsWithValues(fields, values);

            closeConnectionAndCleanup(connection);
        }catch(NullPointerException e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, 
                    "Missing Stomp Configuration,"+
                    " Make Sure Please Make Sure Host, Port And Queue Are Set", e);
        }catch(Exception e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    private BlockingConnection sendFieldsWithValues(String [] fields, String [] values) throws Exception {
        BlockingConnection connection = this.getConnection();
        if(fields.length != values.length){
            throw new Exception("Mismatched Field and Value Sets fields.length != values.length" );
        }
        
        for(int ii = 0 ; ii < fields.length ; ++ii){
            this.workitem.put(fields[ii], values[ii]);
        }
        
        this.sendCurrentWorkitem(connection);
        
        return connection;
    }
    
    private void sendCurrentWorkitem(BlockingConnection connection) throws IOException{
        StompFrame frame = new StompFrame(SEND);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader(this.stompConfig.get("queue").toString()));
        Buffer buffer = new Buffer(this.workitem.toJSONString().getBytes());
        frame.content(buffer);

        connection.send(frame);
        
        try {
            Thread.sleep(500);
        } catch (InterruptedException ex) {
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    private BlockingConnection getConnection()throws IOException, URISyntaxException{
        
        Stomp stomp = new Stomp(this.stompConfig.get("host").toString(), Integer.parseInt(this.stompConfig.get("port").toString()));
        BlockingConnection connection = stomp.connectBlocking();
        
        return connection;
    }

    private void closeConnectionAndCleanup(BlockingConnection connection) throws IOException{
        connection.suspend();
        connection.close();
        
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
    public void setError(String error){
        ((JSONObject)getWorkitem().get("fields")).put("__error__", error);
    }
    
    /**
     * Helper method for getting the error field
     */
    public String getError(){
        return getField( "__error__" );
    }

    /**
     * Helper method for getting the fields
     * 
     * @param field key to get value for
     * @return field value
     */
    public String getField(String field){
        if(((JSONObject)getWorkitem().get("fields")).get(field) == null){
            return null;
        }
        return ((JSONObject)getWorkitem().get("fields")).get(field).toString();
        
    }
    
    public Map perform(String methodName, Map workitem) throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, ParseException{
        try{
            JSONParser parser = new JSONParser();
            String json = JSONObject.toJSONString(workitem);
            setWorkitem((JSONObject)parser.parse(json));

            Method method = getClass().getMethod(methodName);
            method.invoke(this);


        
        } catch (Exception e) {
            this.writeOutput("Task Failed: " + e.toString());
            this.setError("Task Failed: " + e.toString());
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
     * @param value string value to apply to field
     * 
     */
    public void setField(String name, String value){
        ((JSONObject)getWorkitem().get("fields")).put(name, value);
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
    
    public Map getStompConfig() {
        return stompConfig;
    }

    public void setStompConfig(Map stompConfig) {
        this.stompConfig = stompConfig;
    }

    
    /*
     * Database 
     */
    
    public void updateFieldsInRecord(String model, String nameOrId, String field, String value) {
        try{
            
            String [] fields = {"__persist__", "__update__", "__model__", "__record_id__", "__record_field__", "__record_value__"};
            String [] values = {String.valueOf(true), String.valueOf(true), model, nameOrId, field, value};
            BlockingConnection connection = sendFieldsWithValues(fields, values);

            closeConnectionAndCleanup(connection, fields);
            
            
        }catch(NullPointerException e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, 
                    "Missing Stomp Configuration,"+
                    " Make Sure Please Make Sure Host, Port And Queue Are Set");
        }catch(Exception e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    private void closeConnectionAndCleanup(BlockingConnection connection, String [] fields) throws Exception {
        this.closeConnectionAndCleanup(connection);
        for(String field : fields){
            this.workitem.remove(field);
        }
    }

    void createRecordWithFields(String model, String[] recordFields, String[] recordValues) {
         try{
            
            String [] fields = {"__persist__", "__create__", "__model__", "__record_fields__", "__record_values__"};
            String [] values = {String.valueOf(true), String.valueOf(true), model, StringUtils.join(recordFields, ","), StringUtils.join(recordValues, ",")};
            BlockingConnection connection = sendFieldsWithValues(fields, values);

            closeConnectionAndCleanup(connection, fields);
            
            
        }catch(NullPointerException e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, 
                    "Missing Stomp Configuration,"+
                    " Make Sure Please Make Sure Host, Port And Queue Are Set");
        }catch(Exception e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    void deleteRecord(String model, String nameOrId) {
        try{
            
            String [] fields = {"__persist__", "__delete__", "__model__", "__name__"};
            String [] values = {String.valueOf(true), String.valueOf(true), model, nameOrId};
            BlockingConnection connection = sendFieldsWithValues(fields, values);

            closeConnectionAndCleanup(connection, fields);
            
            
        }catch(NullPointerException e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, 
                    "Missing Stomp Configuration,"+
                    " Make Sure Please Make Sure Host, Port And Queue Are Set");
        }catch(Exception e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, e);
        }
    }

   /*
    * End Database 
    */
}
