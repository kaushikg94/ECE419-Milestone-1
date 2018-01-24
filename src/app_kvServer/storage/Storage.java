package storage;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.FileSystemException;
import java.nio.charset.StandardCharsets;

import app_kvServer.IKVServer;

public class Storage implements IStorage {
    private String rootDir;

    /**
	 * Constructs a new Storage object to manage persistent storage.
	 * @param rootDir the root directory to store all data in
	 */
    public Storage(String rootDir) {
        this.rootDir = rootDir;
        new File(rootDir).mkdirs();
    }

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    @Override
    public boolean inStorage(String key) {
        // Check if a file with the specified key exists
        File file = new File(rootDir, key);
        return file.isFile();
    }

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    @Override
    public String getKV(String key) throws Exception {
        if(!inStorage(key)) {
            throw new FileNotFoundException("Specified key not found");
        }

        File file = new File(rootDir, key);
        FileReader fr = new FileReader(file.getPath());
        BufferedReader br = new BufferedReader(fr);
        String value = br.readLine();
        br.close();
        fr.close();
        return value;
    }

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    @Override
    public void putKV(String key, String value) throws Exception {
        // Create file if it does not exist
        File file = new File(rootDir, key);
        if(!file.isFile()) {
            if(!file.createNewFile()) {
                throw new FileSystemException("Unable to create key");
            }
        }

        // Write data to file (overwriting any previous data)
        PrintWriter out = new PrintWriter(file);
        out.print(value);
        out.close();
    }

    /**
     * Delete the key-value pair from storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    @Override
    public void deleteKV(String key) throws Exception {
        if(!inStorage(key)) {
            throw new FileNotFoundException("Specified key not found");
        }
        File file = new File(rootDir, key);
        if(!file.delete()) {
            throw new FileSystemException("Unable to delete key");
        }
    }

    /**
     * Clear the storage of the server
     */
    @Override
    public void clear() throws Exception {
        // Delete all files in root directory
        File rootDirectory = new File(rootDir);
        for(File file: rootDirectory.listFiles()) {
            if(!file.delete()) {
                throw new FileSystemException("Unable to delete key");
            }
        }
    }
}
