package com.maestrodev;

import org.fusesource.hawtbuf.Buffer;
import java.util.HashMap;
import java.io.IOException;
import java.net.URISyntaxException;
import org.fusesource.stomp.client.BlockingConnection;
import org.fusesource.stomp.client.Stomp;
import org.fusesource.stomp.codec.StompFrame;
import org.json.simple.JSONObject;
import static org.fusesource.stomp.client.Constants.*;

/**
 * Main Class for Maestro Plugins written in Java.
 *
 */
public class MaestroWorker 
{
    private JSONObject workitem;
    private HashMap stompConfig;
    
    public MaestroWorker(JSONObject workitem, HashMap stompConfig){
        this.workitem = workitem;
        this.stompConfig = stompConfig;
    }
    
    /**
     * Helper that sends output strings to server for persistence.
     * 
     * @param output - Message to be persisted for the associated TaskExecution
     */
    public void writeOutput(String output) throws IOException, URISyntaxException{
        BlockingConnection connection = getConnection();
        
        sendStringWithConnection(output, connection);

        closeConnectionAndCleanup(connection);
    }
    
    private void sendStringWithConnection(String output, BlockingConnection connection) throws IOException{
        this.workitem.put("__output__", output);
        this.workitem.put("__streaming__", true);
        StompFrame frame = new StompFrame(SEND);
        frame.addHeader(DESTINATION, StompFrame.encodeHeader((String)this.stompConfig.get("queue")));
        Buffer buffer = new Buffer(this.workitem.toJSONString().getBytes());
        frame.content(buffer);

        connection.send(frame);
    }
    
    private BlockingConnection getConnection()throws IOException, URISyntaxException{
        
        Stomp stomp = new Stomp(this.stompConfig.get("host").toString(), Integer.parseInt((String)this.stompConfig.get("port")));
        BlockingConnection connection = stomp.connectBlocking();
        
        return connection;
    }

    private void closeConnectionAndCleanup(BlockingConnection connection) throws IOException{
        connection.close();
        this.workitem.remove("__output__");
        this.workitem.remove("__streaming__");
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
    
    
}
