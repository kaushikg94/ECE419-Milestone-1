package cache;

import app_kvServer.IKVServer;

public interface ICache {
    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in cache.
     * NOTE: does not modify any other properties
     * @return  true if key in cache, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key, assume caller has already checked
     * that it exists before calling
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Delete the key-value pair from storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void deleteKV(String key) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clear();
}
