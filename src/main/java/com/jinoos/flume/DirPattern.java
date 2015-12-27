package com.jinoos.flume;

import java.util.regex.Pattern;

/**
 * <pre>
 * 
 * @author Hayden<br>
 * @date 2015年12月21日 上午11:40:59
 * <br>
 */
public class DirPattern {
	private String path;
	private Pattern filePattern;
	private String topic;

	public DirPattern() {

	}

	public DirPattern(String path, Pattern filePattern, String topic) {
		this.path = path;
		this.filePattern = filePattern;
		this.topic = topic;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Pattern getFilePattern() {
		return filePattern;
	}

	public void setFilePattern(Pattern filePattern) {
		this.filePattern = filePattern;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public String toString() {
		return "path:" + path + ",topic:" + topic + ",filePattern:" + filePattern;
	}
}