package com.maestrodev;

import java.io.IOException;
import java.net.URISyntaxException;

import org.fusesource.stomp.client.BlockingConnection;
import org.fusesource.stomp.client.Stomp;

public class StompConnectionFactory
{

    private static StompConnectionFactory theInstance;
    
    public static StompConnectionFactory getInstance() {
        
        if (theInstance == null) {            
            synchronized (StompConnectionFactory.class) {
                if (theInstance == null)
                    theInstance = new StompConnectionFactory();                
            }
        }
        return theInstance;
    }
    
    
    public BlockingConnection getConnection(String host, int port) throws IOException, URISyntaxException {

        Stomp stomp = new Stomp( host, port );
        BlockingConnection connection = stomp.connectBlocking();
        
        return connection;
    }
}
