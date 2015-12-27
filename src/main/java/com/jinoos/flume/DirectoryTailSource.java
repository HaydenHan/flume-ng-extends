package com.jinoos.flume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <pre>
 * 自定义source,监控指定目录中指定的文件
 * @author Hayden<br>
 * @date 2015年12月19日 上午11:29:41
 * <br>
 */
public class DirectoryTailSource extends AbstractSource implements Configurable, EventDrivenSource {
	private static final String CONFIG_SEPERATOR = ".";
	private static final String CONFIG_DIRS = "dirs";
	private static final String CONFIG_PATH = "path";
	private static final String CONFIG_FILE_PATTERN = "file-pattern";
	private static final String CONFIG_TOPIC = "topic";
	private static final String CONFIG_KEEP_BUFFER_INTERVAL = "keep-buffer-interval";
	private static final String CONFIG_FILE_MONITOR_SLEEP_TIME = "file-monitor-sleep-time";
	private static final String CONFIG_SOURCE_BATCH_SIZE = "batchSize";
	private static final String CONFIG_LOACL_CACHE_DIR = "cacheDir";

	private static final String DEFAULT_FILE_PATTERN = "^(.*)$";
	private static final long DEFAULT_KEEP_BUFFER_INTERVAL = 1000;
	private static final long DEFAULT_FILE_MONITOR_SLEEP_TIME = 500;
	private static final int DEFAULT_SOURCE_BATCH_SIZE = 10;
	private static final String DEFAULE_CONFIG_LOACL_CACHE_DIR = ".flume/source";

	private static final Logger logger = LoggerFactory.getLogger(DirectoryTailSource.class);

	private SourceCounter sourceCounter;

	private String confDirs;

	private String cacheDir;

	private Map<String, DirPattern> dirMap;
	private Map<String, DirPattern> pathMap;

	private ExecutorService executorService;
	private MonitorRunnable monitorRunnable;
	private Future<?> monitorFuture;

	private long keepBufferInterval;
	private long fileMonitorSleepTime;

	// DirectoryTailParserModulable parserModule;

	private DefaultFileMonitor fileMonitor;

	private static final int eventQueueWorkerSize = 10;
	private static final int maxEventQueueSize = 1000 * 1000;
	private BlockingQueue<DirectoryTailEvent> eventQueue = new LinkedBlockingQueue<DirectoryTailEvent>(
			maxEventQueueSize);
	private Future<?>[] workerFuture = new Future<?>[eventQueueWorkerSize];

	private FileSystemManager fsManager;
	private Hashtable<String, FileSet> fileSetMap;

	private Map<String, Long> fileTickMap = new ConcurrentHashMap<String, Long>();
	private int sourceBatchSize;

	/**
	 * <PRE>
	 * 1. MethodName : configure
	 * 2. ClassName  : DirectoryTailSource
	 * 3. Comment   : 读取配置
	 * </PRE>
	 * 
	 * @param context
	 */
	public void configure(Context context) {
		logger.info("Source Configuring..");

		dirMap = new HashMap<String, DirPattern>();
		pathMap = new HashMap<String, DirPattern>();

		keepBufferInterval = context.getLong(CONFIG_KEEP_BUFFER_INTERVAL, DEFAULT_KEEP_BUFFER_INTERVAL);

		fileMonitorSleepTime = context.getLong(CONFIG_FILE_MONITOR_SLEEP_TIME, DEFAULT_FILE_MONITOR_SLEEP_TIME);

		sourceBatchSize = context.getInteger(CONFIG_SOURCE_BATCH_SIZE, DEFAULT_SOURCE_BATCH_SIZE);

		cacheDir = context.getString(CONFIG_LOACL_CACHE_DIR, DEFAULE_CONFIG_LOACL_CACHE_DIR);

		confDirs = context.getString(CONFIG_DIRS).trim();
		Preconditions.checkState(confDirs != null, "Configuration must be specified directory(ies).");

		String[] confDirArr = confDirs.split(" ");

		Preconditions.checkState(confDirArr.length > 0, CONFIG_DIRS + " must be specified at least one.");

		for (int i = 0; i < confDirArr.length; i++) {

			String path = context.getString(CONFIG_DIRS + CONFIG_SEPERATOR + confDirArr[i] + CONFIG_SEPERATOR
					+ CONFIG_PATH);
			if (path == null) {
				logger.error("Configuration is empty : " + CONFIG_DIRS + CONFIG_SEPERATOR + confDirArr[i]
						+ CONFIG_SEPERATOR + CONFIG_PATH);
				continue;
			}

			String topic = context.getString(CONFIG_DIRS + CONFIG_SEPERATOR + confDirArr[i] + CONFIG_SEPERATOR
					+ CONFIG_TOPIC);
			if (topic == null || topic.length() <= 0) {
				logger.error("Configuration is empty : " + CONFIG_DIRS + CONFIG_SEPERATOR + confDirArr[i]
						+ CONFIG_SEPERATOR + CONFIG_TOPIC);
				continue;
			}

			String patternString = context.getString(CONFIG_DIRS + CONFIG_SEPERATOR + confDirArr[i] + CONFIG_SEPERATOR
					+ CONFIG_FILE_PATTERN, DEFAULT_FILE_PATTERN);

			Pattern pattern = null;
			try {
				pattern = Pattern.compile(patternString);
			} catch (PatternSyntaxException e) {
				logger.warn("Configuration has wrong file pattern, " + CONFIG_DIRS + "." + confDirArr[i] + "."
						+ CONFIG_FILE_PATTERN + ":" + patternString);
				logger.warn("Directory will be set default file pattern, " + DEFAULT_FILE_PATTERN);

				pattern = Pattern.compile(patternString);
			}

			DirPattern dir = new DirPattern(path, pattern, topic);

			dirMap.put(confDirArr[i], dir);
			logger.warn("parsed dirs configure dir : " + confDirArr[i] + ", dir : " + dir);
		}
		logger.warn("read source cache:" + cacheDir);
		Util.createCacheFile(cacheDir);
		Util.readRecordFile(cacheDir, fileTickMap);
	}

	/**
	 * <PRE>
	 * 1. MethodName : start
	 * 2. ClassName  : WasAppLogSource
	 * 3. Comment   :
	 * </PRE>
	 */
	@Override
	public void start() {
		logger.info("Source Starting..");

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}

		fileSetMap = new Hashtable<String, FileSet>();

		try {
			fsManager = VFS.getManager();
		} catch (FileSystemException e) {
			logger.error(e.getMessage(), e);
			return;
		}

		monitorRunnable = new MonitorRunnable();

		fileMonitor = new DefaultFileMonitor(monitorRunnable);
		fileMonitor.setRecursive(false);

		FileObject fileObject;

		logger.warn("Dirlist count " + dirMap.size());
		for (Entry<String, DirPattern> entry : dirMap.entrySet()) {
			logger.warn("Scan dir " + entry.getKey());

			DirPattern dirPattern = entry.getValue();

			try {
				fileObject = fsManager.resolveFile(dirPattern.getPath());
			} catch (FileSystemException e) {
				logger.error(e.getMessage(), e);
				continue;
			}

			try {
				if (!fileObject.isReadable()) {
					logger.warn("No have readable permission, " + fileObject.getURL());
					continue;
				}

				if (FileType.FOLDER != fileObject.getType()) {
					logger.warn("Not a directory, " + fileObject.getURL());
					continue;
				}

				fileMonitor.addFile(fileObject);
				logger.warn(fileObject.getName().getPath() + " directory has been add in monitoring list");
				pathMap.put(fileObject.getName().getPath(), entry.getValue());
				// FileSet fileSet = new FileSet(this, fileObject, CONFIG_TOPIC, dirPattern.getTopic());
				// fileSetMap.put(fileObject.getName().getPath(), fileSet);
			} catch (FileSystemException e) {
				logger.warn(e.getMessage(), e);
				continue;
			} catch (Exception e) {
				logger.warn(e.getMessage(), e);
			}

		}

		executorService = Executors.newFixedThreadPool(eventQueueWorkerSize + 1);
		monitorFuture = executorService.submit(monitorRunnable);

		for (int i = 0; i < eventQueueWorkerSize; i++) {
			workerFuture[i] = executorService.submit(new WorkerRunnable(this));
		}

		sourceCounter.start();
		super.start();
	}

	/**
	 * <PRE>
	 * 1. MethodName : stop
	 * 2. ClassName  : WasAppLogSource
	 * 3. Comment   :
	 * </PRE>
	 */
	@Override
	public void stop() {
		logger.info("Source Stopping..");
		fileMonitor.stop();
		sourceCounter.stop();
	}

	private class WorkerRunnable implements Runnable {
		private AbstractSource source;

		private WorkerRunnable(AbstractSource source) {
			this.source = source;
		}

		public void run() {
			while (true) {
				try {
					// DirectoryTailEvent event = eventQueue.poll(
					// eventQueueWorkerTimeoutMiliSecond,
					// TimeUnit.MILLISECONDS);
					DirectoryTailEvent event = eventQueue.take();

					if (event == null) {
						continue;
					}

					if (event.type == FileEventType.FILE_CHANGED) {
						fileChanged(event.event);
					} else if (event.type == FileEventType.FILE_CREATED) {
						fileCreated(event.event);
					} else if (event.type == FileEventType.FILE_DELETED) {
						fileDeleted(event.event);
					} else if (event.type == FileEventType.FLUSH) {
						if (event.fileSet != null) {
							sendEvent(event.fileSet);
						}
					}
				} catch (InterruptedException e) {
					logger.warn(e.getMessage(), e);
				} catch (FileSystemException e) {
					logger.info(e.getMessage(), e);
				}
			}
		}

		private void fileCreated(FileChangeEvent event) throws FileSystemException {
			String path = event.getFile().getName().getPath();
			String dirPath = event.getFile().getParent().getName().getPath();

			logger.warn(path + " has been created.");

			DirPattern dirPattern = null;
			dirPattern = pathMap.get(dirPath);

			if (dirPattern == null) {
				logger.warn("Occurred create event from un-indexed directory. " + dirPath);
				return;
			}

			if (!Util.isInFilePattern(event.getFile(), dirPattern.getFilePattern())) {
				logger.warn(path + " is not in file pattern.");
				return;
			}

			FileSet fileSet;

			fileSet = fileSetMap.get(event.getFile().getName().getPath());

			if (fileSet == null) {
				try {
					logger.info(path + " is not in monitoring list. It's going to be listed.");
					long index = fileTickMap.get(path) == null ? 0L : fileTickMap.get(path);
					fileSet = new FileSet(source, event.getFile(), CONFIG_TOPIC, dirPattern.getTopic(), index);
					synchronized (fileSetMap) {
						fileSetMap.put(path, fileSet);
					}
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
					return;
				}
			}
		}

		private void fileDeleted(FileChangeEvent event) throws FileSystemException {
			String path = event.getFile().getName().getPath();
			String dirPath = event.getFile().getParent().getName().getPath();

			logger.warn(path + " has been deleted.");

			DirPattern dirPattern = pathMap.get(dirPath);
			if (dirPattern == null) {
				logger.warn("Occurred delete event from un-indexed directory. " + dirPath);
				return;
			}

			if (!Util.isInFilePattern(event.getFile(), dirPattern.getFilePattern())) {
				logger.warn(path + " is not in file pattern.");
				return;
			}

			FileSet fileSet = fileSetMap.get(path);

			if (fileSet != null) {
				synchronized (fileSetMap) {
					fileSetMap.remove(path);
				}
				logger.warn("Removed monitoring fileSet.");
			}
			if (fileTickMap.containsKey(path)) {
				fileTickMap.remove(path);
			}
		}

		private void fileChanged(FileChangeEvent event) throws FileSystemException {
			String path = event.getFile().getName().getPath();
			String dirPath = event.getFile().getParent().getName().getPath();

			logger.warn(path + " has been changed.");

			DirPattern dirPattern = pathMap.get(dirPath);
			if (dirPattern == null) {
				logger.warn("Occurred change event from un-indexed directory. " + dirPath);
				return;
			}
			// logger.warn("dirPattern:" + dirPattern);
			// 变化的文件是否是监控数据源
			if (!Util.isInFilePattern(event.getFile(), dirPattern.getFilePattern())) {
				logger.warn("Not in file pattern, " + path);
				return;
			}

			FileSet fileSet = fileSetMap.get(event.getFile().getName().getPath());

			if (fileSet == null) {
				logger.warn(path + " is not in monitoring list.");
				try {
					long index = fileTickMap.get(path) == null ? 0L : fileTickMap.get(path);
					logger.warn("read index:" + index);
					fileSet = new FileSet(source, event.getFile(), CONFIG_TOPIC, dirPattern.getTopic(), index);
					logger.warn("fileSet:" + fileSet.getHeader("topic"));
					synchronized (fileSetMap) {
						fileSetMap.put(path, fileSet);
					}
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
					return;
				}
				// return;
			}

			// logger.warn("start read file:" + path);
			readMessage(path, fileSet);
		}

		// 读取文件数据
		private void readMessage(String path, FileSet fileSet) {

			try {
				String buffer;

				synchronized (fileSet) {

					while ((buffer = fileSet.readLine()) != null) {
						// logger.warn("read line:" + buffer);
						if (buffer.length() == 0) {
							continue;
						}
						if (fileSet.getLineSize() < sourceBatchSize) {
							fileSet.appendLine(buffer);
						} else {
							sendEvent(fileSet);
						}
					}
					if (fileSet.getLineSize() > 0) {
						sendEvent(fileSet);
					}
				}
			} catch (IOException e) {
				logger.warn(e.getMessage(), e);
			}
		}

		private void sendEvent(FileSet fileSet) {
			if (fileSet.getBufferList().isEmpty())
				return;

			synchronized (fileSet) {
				// StringBuffer sb = fileSet.getAllLines();
				// Event event = EventBuilder.withBody(String.valueOf(sb).getBytes(), fileSet.getHeaders());
				List<Event> events = new ArrayList<Event>();
				for (String item : fileSet.getBufferList()) {
					Event event = EventBuilder.withBody(item.getBytes(), fileSet.getHeaders());
					events.add(event);
					// logger.warn("send to channel:" + item);
				}
				// source.getChannelProcessor().processEvent(event);
				source.getChannelProcessor().processEventBatch(events);
				sourceCounter.incrementEventReceivedCount();
				// 记录文件读取位置
				try {
					fileTickMap.put(fileSet.getPath(), fileSet.getReadSeek());
				} catch (IOException e) {
					e.printStackTrace();
				}
				Util.writeRecordFile(cacheDir, fileTickMap);
				fileSet.clear();
			}
		}
	}

	private class MonitorRunnable implements Runnable, FileListener {
		public void run() {

			fileMonitor.setDelay(fileMonitorSleepTime);
			fileMonitor.start();

			while (true) {
				try {
					Thread.sleep(keepBufferInterval);
					fileMonitor.run();
				} catch (InterruptedException e) {
					logger.warn(e.getMessage(), e);
				}

				flushFileSetBuffer();
			}
		}

		// 创建文件
		public void fileCreated(FileChangeEvent event) throws Exception {
			DirectoryTailEvent dtEvent = new DirectoryTailEvent(event, FileEventType.FILE_CREATED);
			eventQueue.put(dtEvent);
		}

		// 删除
		public void fileDeleted(FileChangeEvent event) throws Exception {
			DirectoryTailEvent dtEvent = new DirectoryTailEvent(event, FileEventType.FILE_DELETED);
			eventQueue.put(dtEvent);
		}

		// 修改
		public void fileChanged(FileChangeEvent event) throws Exception {
			DirectoryTailEvent dtEvent = new DirectoryTailEvent(event, FileEventType.FILE_CHANGED);
			eventQueue.put(dtEvent);
		}

		public void flush(FileSet fileSet) {
			DirectoryTailEvent dtEvent = new DirectoryTailEvent(fileSet);
			try {
				eventQueue.put(dtEvent);
			} catch (InterruptedException e) {
				logger.warn(e.getMessage(), e);
			}
		}

		private void flushFileSetBuffer() {
			synchronized (fileSetMap) {
				long cutTime = System.currentTimeMillis() - keepBufferInterval;

				for (Map.Entry<String, FileSet> entry : fileSetMap.entrySet()) {

					if (entry.getValue().getBufferList().size() > 0 && entry.getValue().getLastAppendTime() < cutTime) {
						flush(entry.getValue());
					}
				}
			}
		}
	}

	private enum FileEventType {
		FILE_CREATED, FILE_CHANGED, FILE_DELETED, FLUSH
	}

	private class DirectoryTailEvent {
		FileChangeEvent event;
		FileEventType type;
		FileSet fileSet;

		public DirectoryTailEvent(FileChangeEvent event, FileEventType type) {
			this.type = type;
			this.event = event;
			this.fileSet = null;
		}

		public DirectoryTailEvent(FileSet fileSet) {
			this.type = FileEventType.FLUSH;
			this.fileSet = fileSet;
			this.event = null;
		}
	}
}
