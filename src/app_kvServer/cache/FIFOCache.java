package cache;

import java.util.Hashtable;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer;

public class FIFOCache implements ICache {

	private static Logger logger = Logger.getRootLogger();

    private int cacheSize;
    private Hashtable<String, String> cache;
    private int nEntriesInCache;
    private LinkedList<String> fifoOrder;

    public FIFOCache(int cacheSize) {
        logger.info("Initializing FIFO cache with " + cacheSize + " entries");
        this.cacheSize = cacheSize;
        cache = new Hashtable<String, String>();
        nEntriesInCache = 0;
        fifoOrder = new LinkedList<String>();
    }

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize() {
        return this.cacheSize;
    }

    /**
     * Check if key is in cache.
     * NOTE: does not modify any other properties
     * @return  true if key in cache, false otherwise
     */
    public boolean inCache(String key) {
        return cache.containsKey(key);
    }

    /**
     * Get the value associated with the key, assume caller has already checked
     * that it exists before calling
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception {
        logger.info("Getting from cache: " + key);
        return cache.get(key);
    }

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception {
        // If already in cache, just update
        if(inCache(key)) {
            logger.info("Updating in cache: " + key);
            cache.put(key, value);
            return;
        }

        // Check if we need to evict an entry first
        if(nEntriesInCache >= cacheSize) {
            String keyToEvict = fifoOrder.removeFirst();
            logger.info("Evicting from cache: " + keyToEvict);
            cache.remove(keyToEvict);
            nEntriesInCache--;
        }

        logger.info("Inserting into cache: " + key);
        cache.put(key, value);
        nEntriesInCache++;
        fifoOrder.addLast(key);
    }

    /**
     * Delete the key-value pair from storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void deleteKV(String key) throws Exception {
        logger.info("Deleting from cache: " + key);
        cache.remove(key);
        nEntriesInCache--;
        fifoOrder.remove(key);
    }

    /**
     * Clear the local cache of the server
     */
    public void clear() {
        logger.info("Clearing cache");
        cache.clear();
        nEntriesInCache = 0;
        fifoOrder.clear();
    }
}
