/*
 * Copyright (c) 2013, MaestroDev. All rights reserved.
 */
package com.maestrodev.maestro.plugins;

import java.io.IOException;
import java.net.URISyntaxException;

import org.fusesource.stomp.client.BlockingConnection;
import org.fusesource.stomp.client.Stomp;
/**
 * A stomp connection factory. Used to obtained connections to the stomp server.
 */
public class StompConnectionFactory {

    private static StompConnectionFactory theInstance;

    /**
     * Returns the StompConnectionFactory singleton instance.
     * 
     * @return the StompConnectionFactory singleton instance.
     */
    public static StompConnectionFactory getInstance() {

	if (theInstance == null) {
	    synchronized (StompConnectionFactory.class) {
		if (theInstance == null)
		    theInstance = new StompConnectionFactory();
	    }
	}
	return theInstance;
    }

    /**
     * Gets a stomp connection.
     * 
     * @param host the stomp host to connect to.
     * @param port the port to connect to.
     * @return a stomp connection.
     * @throws IOException if a connection could not be made.
     * @throws URISyntaxException if the host or port are missing or invalid.
     */
    public BlockingConnection getConnection(String host, int port)
	    throws IOException, URISyntaxException {

	Stomp stomp = new Stomp(host, port);
	BlockingConnection connection = stomp.connectBlocking();

	return connection;
    }
}
