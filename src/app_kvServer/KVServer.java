package app_kvServer;

import logger.LogSetup;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.net.BindException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cache.ICache;
import cached_storage.CachedStorage;

public class KVServer implements IKVServer {

	private static final String PERSISTENT_STORAGE_ROOT_DIR = "data";

	private static Logger logger = Logger.getRootLogger();

	private int port;

	private CachedStorage cachedStorage;

    private ServerSocket serverSocket;
    private boolean isRunning;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the
	 * 			 cache is full and there is a GET- or PUT-request on a key that
	 *           is currently not contained in the cache. Options are "FIFO",
	 *           "LRU", and "LFU".
	 */
	public KVServer(int port, CacheStrategy cacheStrategy, int cacheSize) {
		this.port = port;
		this.cachedStorage = new CachedStorage(PERSISTENT_STORAGE_ROOT_DIR,
			cacheStrategy, cacheSize);
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
    public String getHostname() {
		if(this.serverSocket != null) {
			return this.serverSocket.getInetAddress().getHostName();
		}
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy() {
		return cachedStorage.getCacheStrategy();
	}

	@Override
    public int getCacheSize() {
		return cachedStorage.getCacheSize();
	}

	@Override
    public boolean inStorage(String key) {
		return cachedStorage.inStorage(key);
	}

	@Override
    public boolean inCache(String key) {
		return cachedStorage.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception {
		return cachedStorage.getKV(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception {
		// Check if we're doing a regular insert/update or delete
		if(value == null || value.isEmpty()) {
			cachedStorage.deleteKV(key);
		} else {
			cachedStorage.putKV(key, value);
		}
	}

	@Override
    public void clearCache() {
		cachedStorage.clearCache();
	}

	@Override
    public void clearStorage() {
		cachedStorage.clearStorage();
	}

	@Override
    public void run() {
		isRunning = initializeServer();

        if(serverSocket != null) {
	        while(this.isRunning) {
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client, this);
	                new Thread(connection).start();
	                
	                logger.info("Connected to " +
	                		client.getInetAddress().getHostName() +
	                		" on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Unable to establish connection", e);
	            }
	        }
		}

        logger.info("Server stopped");
	}

	@Override
    public void kill() {
		isRunning = false;
		// TODO is this sufficient? Should we Thread.currentThread().stop()?
	}

	@Override
    public void close() {
		isRunning = false;
        try {
			if(serverSocket != null) {
				serverSocket.close();
			}
		} catch (IOException e) {
			logger.error("Unable to close socket on port: " + port, e);
		}
	}

    private boolean initializeServer() {
		logger.info("Initializing server socket");
    	try {
            this.serverSocket = new ServerSocket(this.port);
			logger.info("Server listening on port: " +
				serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Unable to open server socket");
            if(e instanceof BindException) {
            	logger.error("Port already bound: " + port);
            }
            return false;
        }
    }
    
    /**
     * Main entry point for the KV server application. 
     * @param args contains the port number at args[0], cache size (# entries)
	 * at args[1], and cache strategy (one of: None, LRU, LFU, FIFO) at args[2]
     */
    public static void main(String[] args) {
    	try {
			// Start logging
			new LogSetup("logs/server.log", Level.ALL);

			// Check argument count
			if(args.length != 3) {
				System.out.println("Error: Invalid number of arguments");
				System.out.println("Usage: Server <port> <cache size> " +
					"<cache strategy>");
				return;
			}

			// Parse arguments
			int port = Integer.parseInt(args[0]);

			int cacheSize = Integer.parseInt(args[1]);

			IKVServer.CacheStrategy cacheStrategy;
			switch(args[2]) {
				case "LRU":
					cacheStrategy = CacheStrategy.LRU;
					break;
				case "LFU":
					cacheStrategy = CacheStrategy.LFU;
					break;
				case "FIFO":
					cacheStrategy = CacheStrategy.FIFO;
					break;
				default:
					System.out.println("Error: Invalid cache strategy " +
						"(should be one of LRU, LFU, or FIFO)");
					return;
			}

			// Start server
			new KVServer(port, cacheStrategy, cacheSize).run();

		} catch (IOException e) {
			System.out.println("Error: Unable to initialize logger");
			e.printStackTrace();
			System.exit(1);

		} catch (NumberFormatException e) {
			System.out.println("Error: Invalid argument: <port> or " +
				"<cache size> not a valid number");
			System.out.println("Usage: Server <port> <cache size> " +
				"<cache strategy>");
			System.exit(1);
		}
    }
}
