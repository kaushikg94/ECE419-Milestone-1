package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import client.KVCommInterface;
import client.KVStore;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import java.io.InputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.KVMessage.StatusType;
import serialization.Serialization;

/*	Used as the interface class for the client as 
	required by the ant build file.
*/

public class KVClient implements IKVClient {
	
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient>>> ";
	private static final String PROMPTIN = "KVClient<<< ";

	private BufferedReader stdin;
	private boolean stop = false;
	
	private KVStore client = null;	
	private String serverAddress;
	private int serverPort;


	public void run(){
		
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPTIN);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - KVClient terminated ");
			}
		}

	}

	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");
		String command = tokens[0];
		switch(command){

			case "connect":
				logger.info("User input: connect command");
				handleConnect(tokens);
				break;

			case "disconnect":
				logger.info("User input: disconnect command");
				disconnect();
				break;

			case "put":
				logger.info("User input: put command");
				put(tokens);
				break;

			case "get":
				logger.info("User input: get command");
				get(tokens);
				break;

			case "loglevel":
				logger.info("User input: modifying log level command");
				handleLogLevel(tokens);
				break;

			case "help":
				logger.info("User input: help command");
				printHelp();
				break;

			case "quit":
				logger.info("User input: quit command");
				stop = true;
				disconnect();
				System.out.println(PROMPT + "KVClient successful exit!");
				break;

			default:
				logger.info("User input: invalid command: " + tokens[0]);
				printError("Unknown command");
				printHelp();
				break;
		}

	}

	private void get(String[] tokens){
		if (client == null){
			logger.info("User making get request without a connection to server.");
			System.out.println(PROMPT+"Error: Making a request without server connection.");
			return;
		}

		if (tokens.length != 2){
			logger.info("User making get request with invalid number of parameters.");
			printError("Invalid number of parameters!");
			return;			
		}
		String key = tokens[1];
		
		try{
			this.client.get(key);
		}catch (Exception e){

		}

	}


	private void put(String[] tokens){
		if (client == null){
			logger.info("User making get request without a connection to server");
			printError("Making a request without server connection");
			return;
		}

		if (tokens.length < 2 || tokens.length > 3){
			printError("Invalid number of parameters!");
			return;			
		}

		String key = tokens[1];

		try {			
			this.client.put(key, (tokens.length == 3) ? tokens[2] : null);
		} catch (Exception e) {
			System.out.println("Exceptions");
		}


	}



	private void handleConnect(String[] tokens){
		if (client != null){
			logger.info("User attempting to connect  to a server without disconnecting from previous server");
			printError("Please disconnect first.");
			return;
		}


		if(tokens.length != 3) {
			printError("Invalid number of parameters!");
			return;
		}

		try{
			logger.info("Attempting to make a connection to server");
			serverAddress = tokens[1];
			serverPort = Integer.parseInt(tokens[2]);
			newConnection(serverAddress, serverPort);
		} catch(NumberFormatException nfe) {
			printError("No valid address. Port must be a number!");
			logger.info("Unable to parse argument <port>", nfe);
		} catch (UnknownHostException e) {
			printError("Unknown Host!");
			logger.info("Unknown Host!", e);
		} catch (IOException e) {
			printError("Could not establish connection!");
			logger.warn("Could not establish connection!", e);
		} catch (Exception e){

		}
	}

	private void handleLogLevel(String[] tokens){
		if(tokens.length != 2) {
			printError("Invalid number of parameters!");
			return;
		}

		String level = setLevel(tokens[1]);

		if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
			printError("No valid log level!");
			printPossibleLogLevels();
		} else {
			System.out.println(PROMPT + 
					"Log level changed to level " + level);
		} 
	}


	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	
	private void disconnect() {

		if(client != null) {
			this.client.disconnect();
			System.out.println("Sucessfully Disconnected.");
		} else{
			logger.info("Attempting to disconnect from no connection");
			printError("No connection to disconnect from.");
		}

	}


	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}


	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t\t retrieves the value for the specified key from server \n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t inserts or updates the key value pair in the server's database \n");		
		sb.append(PROMPT).append("put <key>");
		sb.append("\t\t\t deletes the key specified from the server's database \n");		
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel to: \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}



    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
		try{
			this.client = new KVStore(hostname, port);
			// String success_print = "Sucessfully connected to server 
			// at address: " + hostname + " with port number : " + port;				
			this.client.addListener(this);
			this.client.connect();
			System.out.printf("Successful Connection to Server at addresss: %s with port number: %d\n", hostname, port);			
			logger.info("Sucessfully connected to server at address: " + hostname + " at port: " + port);			
			this.client.start();
		} catch (Exception e) {
			logger.info("Failed to connect to Server at address: " + hostname + " with port number: " + port);
			printError("System unable to successfully connect.");
			this.client = null;				
		}

    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return this.client;
    }


    public static void main(String[] args) {

    	try {
			new LogSetup("logs/KVClient.log", Level.ALL);
			KVClient kvclient = new KVClient();
			kvclient.run();
		} catch (IOException e) {
			/* Error cases*/
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }

}
