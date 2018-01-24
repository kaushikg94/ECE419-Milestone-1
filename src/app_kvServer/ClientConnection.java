package app_kvServer;

import java.io.InputStream;
import java.io.InputStreamReader;
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



/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 122880;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	private static final int MAX_REQUEST_LINES = 1024;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private KVServer parentServer;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer parentServer) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.parentServer = parentServer;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			while(isOpen) {
				try {
					handleRequests();
				} catch(IOException e) {
					logger.info("Connection to client lost");
					isOpen = false;
				}
			}

		} catch(IOException e) {
			logger.error("Connection to client could not be established", e);
			
		} finally {
			try {
				if(clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException e) {
				logger.error("Unable to tear down connection to client", e);
			}
		}
	}

	/**
	 * Reads inputs line by line until an empty line is encountered. At this
	 * point all accumulated lines are considered one single request and this
	 * request is attempted to be processed.
	 */
	private void handleRequests() throws IOException {
		BufferedReader inStream =
			new BufferedReader(new InputStreamReader(input));
		
		String lines[] = new String[MAX_REQUEST_LINES];
		int i = 0;
		String line = null;
		while((line = inStream.readLine()) != null) {
			if(line.equals("")) {
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
			logger.error("Unable to unserialize request", e);
			KVMessage response = new KVMessageImpl(null,
				"Invalid request", StatusType.GET_ERROR); // TODO generic error?
			sendResponse(response);
			return;
		}

		// Attempt to process the request given its type
		switch(request.getStatus()) {
			case GET:
				handleGetRequest(request);
				break;
			case PUT:
				handlePutRequest(request);
				break;
			default:
				logger.error("Invalid request type: " + request.getStatus());
				KVMessage response = new KVMessageImpl(null,
					"Invalid request type", StatusType.GET_ERROR); // TODO generic error?
				sendResponse(response);
		}
	}

	private void handleGetRequest(KVMessage request) {
		// Attempt to get from cached storage
		String value;
		try {
			value = parentServer.getKV(request.getKey());
		} catch(Exception e) {
			logger.error("Unable to get key-value from cached storage", e);
			KVMessage response = new KVMessageImpl(null,
				"Error while processing request", StatusType.GET_ERROR);
			sendResponse(response);
			return;
		}

		// Upon success, return result in response
		KVMessage response = new KVMessageImpl(request.getKey(), value,
			StatusType.GET_SUCCESS);
		sendResponse(response);
	}

	private void handlePutRequest(KVMessage request) {
		// Attempt to insert into cached storage
		boolean isInStorage;
		try {
			isInStorage = parentServer.inStorage(request.getKey());
			parentServer.putKV(request.getKey(), request.getValue());
		} catch(Exception e) {
			logger.error("Unable to put key-value into cached storage", e);
			StatusType status = request.getValue() == null ?
				StatusType.DELETE_ERROR :
				StatusType.PUT_ERROR;
			KVMessage response = new KVMessageImpl(null,
				"Error while processing request", status);
			sendResponse(response);
			return;
		}

		// Upon success, return response message
		StatusType status = isInStorage ?
			(request.getValue() == null ?
				StatusType.DELETE_SUCCESS : StatusType.PUT_UPDATE) :
			StatusType.PUT_SUCCESS;
		KVMessage response = new KVMessageImpl(request.getKey(),
			request.getValue(), status);
		sendResponse(response);
	}

	private void sendResponse(KVMessage response) {
		String responseString = Serialization.serialize(response);

		try {
			output.write(responseString.getBytes());
			output.flush();
		} catch(IOException e) {
			logger.error("Unable to send response to client", e);
		}
	}
}
