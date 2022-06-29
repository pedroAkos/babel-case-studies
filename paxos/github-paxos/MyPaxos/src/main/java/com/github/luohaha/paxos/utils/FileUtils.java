package com.github.luohaha.paxos.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileUtils {
	/**
	 * Make sure the file exists
	 * @param addr
	 * File address
	 * @throws IOException
	 */
	public static void createFileIfNotExist(String addr) throws IOException {
		File file = new File(addr);
		if (!file.exists()) {
			file.createNewFile();
		}
	}

	/**
	 * Read the contents of the file and return a string
	 * @param filename
	 * @return
	 */
	public static String readFromFile(String filename) {
		String ret = "";
		File file = new File(filename);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            StringBuffer buffer = new StringBuffer();
            while ((tempString = reader.readLine()) != null) {
            		buffer.append(tempString);
            }
            ret = buffer.toString();
            reader.close();
        } catch (IOException e) {
            //e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return ret;
	}

	/**
	 * Write data to file
	 * @param addr
	 * File address
	 * @param data
	 * Address to be written
	 * @throws IOException
	 */
	public static void writeToFile(String addr, String data) throws IOException {
		FileWriter fileWriter = new FileWriter(addr);
		fileWriter.write(data);
		fileWriter.flush();
		fileWriter.close();
	}

}
