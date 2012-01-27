package com.maestrodev;

import java.util.Map;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.fusesource.hawtbuf.Buffer;
import java.io.IOException;
import java.net.URISyntaxException;
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
            
            BlockingConnection connection = getConnection();
            

            sendCancelWithConnection(connection);

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
            
            BlockingConnection connection = getConnection();
            

            sendWaitingWithConnection(waiting, connection);

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
            
            BlockingConnection connection = getConnection();
            

            sendStringWithConnection(output, connection);

            closeConnectionAndCleanup(connection);
        }catch(NullPointerException e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, 
                    "Missing Stomp Configuration,"+
                    " Make Sure Please Make Sure Host, Port And Queue Are Set");
        }catch(Exception e){
            Logger.getLogger(MaestroWorker.class.getName()).log(Level.SEVERE, null, e);
        }
    }
    
    
    private void sendWaitingWithConnection(boolean waiting, BlockingConnection connection) throws IOException{
        this.workitem.put("__waiting__", waiting);
        this.sendCurrentWorkitem(connection);
    }
    
    private void sendCancelWithConnection(BlockingConnection connection) throws IOException{
        this.workitem.put("__cancel__", true);
        this.sendCurrentWorkitem(connection);
    }    
    
    private void sendStringWithConnection(String output, BlockingConnection connection) throws IOException{
        this.workitem.put("__output__", output);
        this.workitem.put("__streaming__", true);
        this.sendCurrentWorkitem(connection);
    }
    
    private void sendCurrentWorkitem(BlockingConnection connection) throws IOException{
        StompFrame frame = new StompFrame(SEND);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader(this.stompConfig.get("queue").toString()));
        Buffer buffer = new Buffer(this.workitem.toJSONString().getBytes());
        frame.content(buffer);

        connection.send(frame);
        
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
        JSONParser parser = new JSONParser();
        String json = JSONObject.toJSONString(workitem);
        setWorkitem((JSONObject)parser.parse(json));
        
        Method method = getClass().getMethod(methodName);
        method.invoke(this);
        
        
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

   
}
