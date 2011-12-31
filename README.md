Maestro Plugin
==============

Maestro 4 provides support for plugins written in Java and Ruby.  The are both permitted to have dependent libraries.  Ruby based plugins provide the worker source file (.rb) and any dependencies (.gem) are loaded using the Rubygems (gem) executable.  Java based plugins provide a java package (.jar) containing worker source, a project object model (pom.xml) with a list of dependencies.  Java dependecies are then loaded using Maven.  Both plugin types provide a manifest file (manifest.json) that details the contents and attributes of the plugin.

Develop a Maestro 4 plugin
--------------------------

This is a simple example that shows how to create an IRC plugin in Java. The example uses netbeans for code editing and to add dependencies.

### Create a Maven Java ###

The first step is to create a Maven Java Application Project. Select New Project from the File menu.  Then Choose Maven - Java Application.

![alt text](http://d.pr/UyeY+ "Create a netbeans project")


Enter your project Information, in this case the project is maestro-irc-plugin, set the location to create the project, and the maven project attributes.

![alt text](http://d.pr/12Zv+ "Create a netbeans project")

This will create a file structure similar to below.  You can rename the templated Source and Test files to IrcWorker and IrcWorkerTest.  The pom.xml and settings.xml file are created similarly.

![alt text](http://d.pr/bO5h+ "Create a netbeans project")


### Adding Dependencies ###

The Irc Plugin project needs to include the the maestro-plugin project and any other dependency in it's pom.xml file.  In this case we are using the irclib library for Irc functionality.  The irclib library has several dependencies of it's own which are listed in the project explorer below.

#### pom.xml ####
```xml
  <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.maestrodev</groupId>
    <artifactId>maestro-irc-plugin</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>maestro-irc-plugin</name>
    <url>http://maven.apache.org</url>

    <properties>
      <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
      <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>3.8.1</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.schwering</groupId>
        <artifactId>irclib</artifactId>
        <version>1.10</version>
      </dependency>
      <dependency>
        <groupId>com.maestrodev</groupId>
        <artifactId>maestro-plugin</artifactId>
        <version>1.0-SNAPSHOT</version>
      </dependency>
      <dependency>
        <groupId>org.mockito</groupId>
        <artifactId>mockito-all</artifactId>
        <version>1.9.0</version>
        <scope>test</scope>
      </dependency>
    </dependencies>
  </project>
```


### Creating the Worker ###

The source consists of the IrcWorker class and the IrcEventListener class which, as you can tell by the name, is required by the irclib for IRC event handling.

#### IrcWorker ####

```java
  package com.maestrodev;

  import org.schwering.irc.lib.IRCConnection;

  /**
   * Hello world!
   *
   */
  public class IrcWorker extends MaestroWorker
  {
      public IrcWorker(){     
          super();
      }
    
      public void postMessage() throws Exception {
        
          final IRCConnection conn = new IRCConnection(
                                      getField("server"), 
                                      Integer.parseInt(getField("port").toString()),
                                      Integer.parseInt(getField("port").toString()) + 1, 
                                      null, 
                                      getField("nickname"), 
                                      getField("nickname"), 
                                      getField("nickname")
                                    ); 
           IrcEventListener eventListener = new IrcEventListener(conn, getField("channel"), getField("body"));
           conn.addIRCEventListener(eventListener);
           conn.setDaemon(true);
           conn.setColors(false); 
           conn.setPong(true); 
         
           try {
             conn.connect(); // Try to connect!!! Don't forget this!!!
           } catch (Exception ioexc) {
             ioexc.printStackTrace(); 
           }

         
           while(!eventListener.wasMessageSent()){         
              Thread.sleep(1000);
           }
         
           writeOutput("The Message Was Sent!");
         
           conn.close();
      }    
  }
```

IrcWorker is a simple class with a no argument constructor and a method called postMessage.  postMessage creates a new IRCConnection object with field values provided (I will explain how to define those in a bit).  The getField method is used to access value inside the current compositions run context.  The connection is created, connected and the method waits for the message to be sent.  A call to writeOutput will put the message "The Message Was Sent!" into the composition runs output stream and stored for access.  Finally the connection to the irc server is closed.


#### IrcEventListener ####

```java
    /*
    * To change this template, choose Tools | Templates
    * and open the template in the editor.
    */
    package com.maestrodev;

    import org.schwering.irc.lib.IRCConnection;
    import org.schwering.irc.lib.IRCEventListener;
    import org.schwering.irc.lib.IRCModeParser;
    import org.schwering.irc.lib.IRCUser;

    class IrcEventListener implements IRCEventListener {

    
        private IRCConnection connection;
        private boolean messageSent;
        private String body, channel;
    
    
        public IrcEventListener(IRCConnection connection, String channel, String body){
            this.connection = connection;
            this.body = body;
            this.channel = channel;
        }
        
        public synchronized boolean wasMessageSent(){
            return messageSent;
        }
    
        public void onRegistered() {
            connection.doJoin(channel);
            messageSent = false;
        }

        public void onJoin(String string, IRCUser ircu) {
            connection.doPrivmsg(channel, body);
            messageSent = true;
        }

        public void onDisconnected() {
        
        }

        public void onError(String string) {
        
        }

        public void onError(int i, String string) {
        
        }

        public void onInvite(String string, IRCUser ircu, String string1) {
        
        }

        public void onKick(String string, IRCUser ircu, String string1, String string2) {
       
        }

        public void onMode(String string, IRCUser ircu, IRCModeParser ircmp) {
       
        }

        public void onMode(IRCUser ircu, String string, String string1) {
       
        }

        public void onNick(IRCUser ircu, String string) {
       
        }

        public void onNotice(String string, IRCUser ircu, String string1) {
       
        }

        public void onPart(String string, IRCUser ircu, String string1) {
       
        }

        public void onPing(String string) {
       
        }

        public void onPrivmsg(String string, IRCUser ircu, String string1) {
       
        }

        public void onQuit(IRCUser ircu, String string) {
       
        }

        public void onReply(int i, String string, String string1) {
       
        }

        public void onTopic(String string, IRCUser ircu, String string1) {
       
        }

        public void unknown(String string, String string1, String string2, String string3) {
       
        }
    
    }
```

The IrcEventListener is a class for handling irc events the constructor takes the connection, channel, and body objects.  The class has a messageSent state variable that allows for monitoring if the message has been sent.  The onRegister message is called when the Irc connection is complete.  The method then joins the supplied channel.  onJoin is called after the join event completes.  Finally the message is sent to the channel with body.

#### IrcWorkerTest ####

```java

package com.maestrodev;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.json.simple.JSONObject;

/**
 * Unit test for simple App.
 */
public class IrcWorkerTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public IrcWorkerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( IrcWorkerTest.class );
    }
    
    /**
     * Test IrcWorker
     */
    public void testIrcWorker() throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
        IrcWorker ircWorker = new IrcWorker();
        JSONObject fields = new JSONObject();
        fields.put("body", "Hello From Maestro 4!");
        fields.put("nickname", "irc-plugin-test");        
        fields.put("server", "irc.freenode.net");
        fields.put("password", null);
        fields.put("ssl", "false");
        fields.put("port", "6667");
        fields.put("channel", "#kittest");        
        
        JSONObject workitem = new JSONObject();
        workitem.put("fields", fields);
        ircWorker.setWorkitem(workitem);
               
        
        Method method = ircWorker.getClass().getMethod("postMessage");
        method.invoke(ircWorker);
        
    }
}



```

The testIrcWorker method has a simple example of how the worker will be used when it is install in Maestro 4.  The fields are set and the method postMessage is called. This will result in the message "Hello From Maestro 4!" getting posted in the #kittest channel on irc.freenode.net.

### Defining the Manifest ###

#### manifest.json ####

```json
[{ 
  "name" : "irc (java)",
  "description" : "IRC Client Logging Written In Java",
  "author": "Kit Plummer",
  "version": "1.0",
  "class": "com.maestrodev.IrcWorker",
  "type":"java",
  "dependencies":[
  	{
		  "name":"maestro-irc-plugin-1.0-SNAPSHOT.jar"
	  },
    {
      "name":"pom.xml"
    }
  ],
  "task":{
    "command" : "/irc/postMessage",    
    "inputs" : {"body" : {"value" : "", "type" : "String", "required" : "true"},
		"nickname":{"value" : "", "type" : "String", "required" : "true"},
		"server":{"value" : "", "type" : "String", "required" : "true"},
		"password":{"value" : "", "type" : "Password", "required" : "false"},
		"ssl":{"value" : "", "type" : "Boolean", "required" : "true"},
		"port":{"value" : "", "type" : "Integer", "required" : "true"},
		"channel":{"value" : "", "type" : "String", "required" : "true"}															
	},
    "outputs" : {},
    "tool_name":"Notification"
  }
}]

```

Note that the json object is an array of hash objects, this allows you to include more than one plugin inside a manifest and zip package.

* name - Name of the plugin, that is display in the Maestro 4 UI
* description - Description of the plugin that is displayed 
* author - Person responsible for such genius
* version - plugins version
* class - The class name of the plugins source worker
* type - java or ruby
* dependencies - an array with jars and or the pom.xml file for loading dependencies. The jars should be located in a jars directory inside the zip archive, while the pom.xml should be at the root.
* task - a hash of attributes that defines the plugin at runtime.
  - command - /:id/:method for the plugin
  - inputs - a hash of values that are expected by the plugin, these will be accessed by the getField(s) methods.
  - outputs - hash of values that will be set by the plugin and can be used in subsequent tasks.
  - tool_name - category definition for the plugin
  
  
### Directory and package structure ###

Maestro 4 expects plugins to be packaged as zip files.  For java based plugins a pom.xml file should be at the root with a jars directory containing any included libs.

#### example structure ####

maestro-irc-plugin.zip

* pom.xml
* jars/maestro-irc-plugin-1.0-SNAPSHOT.jar

This will take the dependencies inside the jar and load them as well as the maestro-irc-plugin-1.0-SNAPSHOT.jar that is created from the above example.

