package com.jinoos.flume;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.vfs2.FileObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
	private static final Logger logger = LoggerFactory.getLogger(Util.class);
	private static FileWriter writer = null;
	private static final String cacheFileName = "scf";

	public static boolean isInFilePattern(FileObject file, Pattern pattern) {
		String fileName = file.getName().getBaseName();
		Matcher matcher = pattern.matcher(fileName);
		if (matcher.find()) {
			return true;
		}
		return false;
	}

	/**
	 * <pre>
	 * 
	 * @param filename
	 * @param fileTickMap
	 * @author Hayden<br>
	 * @date 2015年12月21日 下午5:09:14
	 * <br>
	 */
	public static void readRecordFile(String filename, Map<String, Long> fileTickMap) {
		BufferedReader reader = null;
		try {
			File file = new File(getFileName(filename));
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				try {
					String[] strArr = tempString.split("=", 2);
					fileTickMap.put(strArr[0], Long.parseLong(strArr[1]));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

	public static void writeRecordFile(String filename, Map<String, Long> fileTickMap) {
		FileWriter writer = null;
		try {
			if (writer == null) {
				writer = new FileWriter(getFileName(filename), false);
			}
			for (Map.Entry<String, Long> entry : fileTickMap.entrySet()) {
				String str = entry.getKey() + "=" + entry.getValue();
				writer.write(str + "\n");
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void createCacheFile(String filename) {
		File file = new File(getFileName(filename));
		if (!file.exists()) {
			file.getParentFile().mkdirs();
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static String getFileName(String filename) {
		return filename + "/" + cacheFileName;
	}

	public static void main(String[] args) {
		Util.createCacheFile("D:/test/mkdir/dir/1222");
		Map<String, Long> text = new HashMap<String, Long>();
		text.put("test1", 1L);
		text.put("test2", 2L);
		text.put("test3", 3L);
		writeRecordFile("D:/test/mkdir/dir/1222", text);
		text.put("test2", 5L);
		writeRecordFile("D:/test/mkdir/dir/1222", text);
		Map<String, Long> text2 = new HashMap<String, Long>();
		readRecordFile("D:/test/mkdir/dir/1222", text2);
		for (Map.Entry<String, Long> entry : text2.entrySet()) {
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
			// C4E6B9C229F2BCB00EDDC42FF77D45C20
		}
	}
}
