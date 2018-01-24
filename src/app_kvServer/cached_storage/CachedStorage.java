package cached_storage;

import java.io.IOException;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;
import cache.ICache;
import storage.IStorage;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending.
 */
public class CachedStorage {

	private static Logger logger = Logger.getRootLogger();

    private CacheStrategy cacheStrategy;
    private ICache cache;
    private IStorage storage;
	
	/**
	 * Constructs a new CachedStorage object with the given parameters.
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the
	 * 			 cache is full and there is a GET- or PUT-request on a key that
	 *           is currently not contained in the cache. Options are "FIFO",
	 *           "LRU", and "LFU".
	 */
	public CachedStorage(CacheStrategy cacheStrategy, int cacheSize) {
		this.cacheStrategy = cacheStrategy;

        // Set up cache
        switch(cacheStrategy) {
            case LRU:
                // TODO
                break;
            case LFU:
                // TODO
                break;
            case FIFO:
                //TODO
                break;
        }

        // Set up storage
        // TODO
	}

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy() {
		return this.cacheStrategy;
	}

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize() {
        return cache.getCacheSize();
    }

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key) {
        return storage.inStorage(key);
    }

    /**
     * Check if key is in cache.
     * NOTE: does not modify any other properties
     * @return  true if key in cache, false otherwise
     */
    public boolean inCache(String key) {
        return cache.inCache(key);
    }

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception {
        if(inCache(key)) {
            return cache.getKV(key);
        } else {
            return storage.getKV(key);
        }
    }

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception {
        // Put into persistent storage first
        storage.putKV(key, value);

        // Put into cache if successfully inserted into persistent storage
        cache.putKV(key, value);
    }

    /**
     * Clear the local cache of the server
     */
    public void clearCache() {
        logger.info("Clearing cache");
        cache.clear();
    }

    /**
     * Clear the storage of the server
     */
    public void clearStorage() {
        logger.warn("Clearing persistent storage");
        storage.clear();
    }
}