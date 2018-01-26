package cache;

import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer;

public class LRUCache implements ICache {

	private static Logger logger = Logger.getRootLogger();

    private int cacheSize;
    private Hashtable<String, String> cache;
    private int nEntriesInCache;
    private Hashtable<String, Date> lruTimes;

    public LRUCache(int cacheSize) {
        logger.info("Initializing LRU cache with " + cacheSize + " entries");
        this.cacheSize = cacheSize;
        cache = new Hashtable<String, String>();
        nEntriesInCache = 0;
        lruTimes = new Hashtable<String, Date>();
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
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception {
        logger.info("Getting from cache: " + key);
        lruTimes.put(key, new Date());
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
            lruTimes.put(key, new Date());
            return;
        }

        // Check if we need to evict an entry first
        if(nEntriesInCache >= cacheSize) {
            String keyToEvict = getLruTime();
            logger.info("Evicting from cache: " + keyToEvict +
                " (last used at " + lruTimes.get(keyToEvict) + ")");
            cache.remove(keyToEvict);
            nEntriesInCache--;
            lruTimes.remove(keyToEvict);
        }

        logger.info("Inserting into cache: " + key);
        cache.put(key, value);
        nEntriesInCache++;
        lruTimes.put(key, new Date());
    }

    private String getLruTime() {
        String lfuKey = null;
        for(String key : lruTimes.keySet()) {
            if(lfuKey == null || lruTimes.get(key).before(lruTimes.get(lfuKey))) {
                lfuKey = key;
            }
        }
        return lfuKey;
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
        lruTimes.remove(key);
    }

    /**
     * Clear the local cache of the server
     */
    public void clear() {
        logger.info("Clearing cache");
        cache.clear();
        nEntriesInCache = 0;
        lruTimes.clear();
    }
}
