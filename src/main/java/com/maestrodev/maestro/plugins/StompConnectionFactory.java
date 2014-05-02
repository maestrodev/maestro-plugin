/*
 * Copyright (c) 2013, MaestroDev. All rights reserved.
 */
package com.maestrodev.maestro.plugins;

import java.io.IOException;
import java.net.URI;
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

    /**
     * Gets a stomp connection.
     * 
     * @param uri the stomp URI to connect to.
     * @return a stomp connection.
     * @throws IOException if a connection could not be made.
     * @throws URISyntaxException if the host or port are missing or invalid.
     */
    public BlockingConnection getConnection(String uri)
	    throws IOException, URISyntaxException {

        URI u = new URI(uri);

        String scheme = u.getScheme();
        if (scheme.equals("stomp")) {
          scheme = "tcp";
        } else if (uri.equals("stomp+ssl")) {
          scheme = "ssl";
        }

        int port = 61613;
        if (u.getPort() >= 0) {
          port = u.getPort();
        }

	Stomp stomp = new Stomp(scheme + "://" + u.getHost() + ":" + port);

        // TODO: extract user/pass from URI and set on Stomp class

	BlockingConnection connection = stomp.connectBlocking();

	return connection;
    }
}
