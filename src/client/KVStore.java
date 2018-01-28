package client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.io.FileNotFoundException;

import java.net.Socket;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.KVMessage.StatusType;
import serialization.Serialization;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import serialization.Serialization;
import org.apache.log4j.Logger;
import app_kvClient.KVClient;


public class KVStore extends Thread implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */

	private Logger logger = Logger.getRootLogger();	
	private Set<KVClient> listeners;

	private OutputStream output;
 	private InputStream input;
 	private Socket socket;

 	private String serverAddress;
 	private int serverPort;
	private boolean running;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private static final int MAX_REQUEST_LINES = 1024;


	public KVStore(String address, int port) 
		throws UnknownHostException, IOException{
		// TODO Auto-generated method stub
		this.serverAddress = address;
		this.serverPort = port;
		listeners = new HashSet<KVClient>();
		setRunning(true);

	}

	public boolean isRunning(){
		return this.running;
	}

	public void setRunning(boolean run){
		this.running = run;
	}

	public void addListener(KVClient listener){
		listeners.add(listener);
	}

	public void run(){
		while(isRunning()){
			try {
				handleRequests();
			} catch(IOException e) {
				logger.info("Connection to client lost");
				setRunning(false);
			}

		}
	}

	private void handleRequests() throws IOException {
		BufferedReader inStream =
			new BufferedReader(new InputStreamReader(input));
		
		String lines[] = new String[MAX_REQUEST_LINES];
		int i = 0;
		String line = null;
		while((line = inStream.readLine()) != null) {
			if(line.isEmpty()) {
				String actualLines[] = new String[i];
				for(int j = 0; j < i; j++) {
					actualLines[j] = lines[j];
				}
				handleRequest(actualLines);
				i = 0;
			} else {
				lines[i] = line;
				i++;
			}
		}
	}

	/**
	 * Handles a single request, provided as a list of lines
	 * @param lines the lines in the request (excluding trailing empty line)
	 */
	private void handleRequest(String[] lines) {
		// Attempt to unserialize request from string into KVMessage
		KVMessage request;
		try {
			request = Serialization.unserialize(lines);
		} catch(IllegalArgumentException e) {
			logger.info("Unable to unserialize request: " + e.getMessage());
			KVMessage response = new KVMessageImpl(null,
				"Invalid request", StatusType.GET_ERROR);
			// sendResponse(response);
			return;
		}

		switch(request.getStatus()){
			case GET_ERROR:
				System.out.printf("GET_ERROR: %s\n", request.getValue());
				break;
			case GET_SUCCESS:
				System.out.printf("GET_SUCCESS: key: %s, value: %s\n", request.getKey(), request.getValue());
				break;
			case PUT_SUCCESS:
				System.out.printf("PUT_SUCCESS: key: %s, value: %s\n", request.getKey(), request.getValue());
				break;
			case PUT_UPDATE:
				System.out.printf("PUT_UPDATE: key: %s, value: %s\n", request.getKey(), request.getValue());
				break;
			case PUT_ERROR:
				System.out.printf("PUT_ERROR: %s\n", request.getValue());
				break;
			case DELETE_SUCCESS:
				System.out.printf("DELETE_SUCCESS: key: %s, value: %s\n", request.getKey(), request.getValue());
				break;
			case DELETE_ERROR:
				System.out.printf("DELETE_ERROR: %s\n", request.getValue());			
				break;
			default:
				// Should have been caught in unserialize function
				System.out.println("Server has returned invalid input");
				break;

		}
	}

	public void sendMessage(String msg) throws IOException {
			output.write(msg.getBytes());
			output.flush();
    }


	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		this.socket = new Socket(this.serverAddress, this.serverPort);
		output = socket.getOutputStream();
		input = socket.getInputStream();
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (socket != null) {
		try{
			input.close();
			output.close();
			socket.close();
		} catch (IOException e){

		}
			socket = null;
			logger.info("connection closed!");
		}

	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		KVMessage temp = new KVMessageImpl(key, value, StatusType.PUT);
		String serialized = Serialization.serialize(temp);
		System.out.println(serialized);
		this.sendMessage(serialized);

		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		KVMessage temp = new KVMessageImpl(key, null, StatusType.GET);
		String serialized = Serialization.serialize(temp);
		this.sendMessage(serialized);

		return null;
	}

}
