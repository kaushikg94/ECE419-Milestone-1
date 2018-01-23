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
					logger.warn("Warning: Connection lost");
					isOpen = false;
				}
			}

		} catch(IOException e) {
			logger.error("Error: Connection could not be established", e);
			
		} finally {
			try {
				if(clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException e) {
				logger.error("Error: Unable to tear down connection", e);
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
			lines[i] = line;
			if(line.equals("")) {
				handleRequest(lines);
			}
		}
	}

	private void handleRequest(String[] lines) {
		try {
			// Unserialize request object
			KVMessage request = Serialization.unserialize(lines);

			switch(request.getStatus()) {
				case GET:
					handleGetRequest(request);
					break;
				case PUT:
					handlePutRequest(request);
					break;
				default:
					logger.error("Error: Invalid request type: " +
						request.getStatus());
					KVMessage response = new KVMessageImpl(null,
						"Invalid request type", StatusType.GET_ERROR);
					sendResponse(response);
			}
		} catch(Exception e) {
			logger.error("Error: Unable to unserialize request", e);
			KVMessage response = new KVMessageImpl(null,
				"Unable to read request", StatusType.GET_ERROR);
			try {
				sendResponse(response);
			} catch(Exception e2) {
				logger.error("Error: Unable to send error response", e2);
			}
		}
	}

	private void handleGetRequest(KVMessage request) throws Exception {
		try {
			String value = parentServer.getKV(request.getKey());
			KVMessage response = new KVMessageImpl(request.getKey(), value,
				StatusType.GET_SUCCESS);
			sendResponse(response);

		} catch(Exception e) {
			logger.error("Error: Unable to get KV", e);
			KVMessage response = new KVMessageImpl(null, "Unable to GET",
				StatusType.GET_ERROR);
			sendResponse(response);
		}
	}

	private void handlePutRequest(KVMessage request) throws Exception {
		boolean isInStorage = parentServer.inStorage(request.getKey());
		try {
			parentServer.putKV(request.getKey(), request.getValue());
			StatusType status = isInStorage ?
				(request.getValue().equals("") ?
					StatusType.DELETE_SUCCESS : StatusType.PUT_UPDATE) :
				StatusType.PUT_SUCCESS;
			KVMessage response = new KVMessageImpl(request.getKey(),
				request.getValue(), status);
			sendResponse(response);

		} catch(Exception e) {
			logger.error("Error: Unable to put KV", e);
			StatusType status = request.getValue().equals("") ?
				StatusType.DELETE_ERROR :
				StatusType.PUT_ERROR;
			KVMessage response = new KVMessageImpl(null, "Unable to PUT",
				status);
			sendResponse(response);
		}
	}

	private void sendResponse(KVMessage response) throws IOException {
		String responseString = Serialization.serialize(response);
		output.write(responseString.getBytes());
		output.flush();
	}
}
