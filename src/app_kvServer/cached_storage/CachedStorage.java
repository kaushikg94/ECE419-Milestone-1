package cached_storage;

import java.io.IOException;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;
import cache.ICache;
import cache.LFUCache;
import cache.FIFOCache;
import storage.IStorage;
import storage.Storage;

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
	 * @param storageRootDir root directory to store persistent data to
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the
	 * 			 cache is full and there is a GET- or PUT-request on a key that
	 *           is currently not contained in the cache. Options are "FIFO",
	 *           "LRU", and "LFU".
	 */
    public CachedStorage(String storageRootDir, CacheStrategy cacheStrategy,
            int cacheSize) {
		this.cacheStrategy = cacheStrategy;

        // Set up cache
        switch(cacheStrategy) {
            case LRU:
                // TODO
                break;
            case LFU:
                cache = new LFUCache(cacheSize);
                break;
            case FIFO:
                cache = new FIFOCache(cacheSize);
                break;
            default:
                cache = null;
        }

        // Set up storage
        storage = new Storage(storageRootDir);
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
        if(cache != null) {
            return cache.getCacheSize();
        } else {
            return 0;
        }
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
        if(cache != null) {
            return cache.inCache(key);
        } else {
            return false;
        }
    }

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception {
        if(cache != null && inCache(key)) {
            return cache.getKV(key);
        } else {
            String value = storage.getKV(key);
            cache.putKV(key, value);
            return value;
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
        if(cache != null) {
            cache.putKV(key, value);
        }
    }

    /**
     * Delete the key-value pair from storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void deleteKV(String key) throws Exception {
        // Delete from persistent storage first
        storage.deleteKV(key);

        // Delete from cache if successfully deleted from persistent storage
        if(cache != null) {
            cache.deleteKV(key);
        }
    }

    /**
     * Clear the local cache of the server
     */
    public void clearCache() {
        logger.info("Clearing cache");
        if(cache != null) {
            cache.clear();
        }
    }

    /**
     * Clear the storage of the server
     */
    public void clearStorage() {
        logger.warn("Clearing persistent storage");
        try {
            storage.clear();
        } catch(Exception e) {
            logger.error("Unable to clear persistent storage", e);
        }
    }
}
