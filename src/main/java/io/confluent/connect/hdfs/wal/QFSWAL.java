/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.wal;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.wal.FilePathOffset;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QFSWAL implements WAL {
  private static final String LOCK_FILE_EXTENSION = "lock-qfs";

  private static final Duration DEFAULT_LOCK_REFRESH_INTERVAL = Duration.ofSeconds(10);
  private static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofSeconds(60);
  private final UUID uuid;
  private final Timer timer;
  private final HdfsStorage storage;
  private final String logsDir;
  private final TopicPartition topicPartition;
  private final Pattern pattern;
  private final String lockDir;
  private Lock lock;
  private final Duration lockRefreshInterval;
  private final Duration lockTimeout;
  private static final Logger log = LoggerFactory.getLogger(QFSWAL.class);

  public QFSWAL(String logsDir, TopicPartition topicPartition, HdfsStorage storage) {
    this(logsDir,
        topicPartition,
        storage,
        DEFAULT_LOCK_REFRESH_INTERVAL,
        DEFAULT_LOCK_TIMEOUT
    );
  }

  public QFSWAL(
          String logsDir,
          TopicPartition topicPartition,
          HdfsStorage storage,
          Duration lockRefreshInterval,
          Duration lockTimeout) {
    this.uuid = UUID.randomUUID();
    this.pattern = Pattern.compile(String.format(
            "(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
                    + "-(?<epochInMS>\\d+)(\\.)%s",
            LOCK_FILE_EXTENSION));
    this.timer = new Timer("QFSWAL-Timer", true);
    this.storage = storage;
    this.logsDir = logsDir;
    this.topicPartition = topicPartition;
    this.lockDir = FileUtils.directoryName(storage.url(), logsDir, topicPartition);
    this.lockRefreshInterval = lockRefreshInterval;
    this.lockTimeout = lockTimeout;
    this.lock = this.createOrRenameLock();
    this.startRenamingTimer();
  }

  static class Lock {
    public final String storageURL;
    public final String logsDir;
    public final TopicPartition topicPartition;
    public final UUID uuid;
    public final Instant instant;

    Lock(
            String storageURL,
            String logsDir,
            TopicPartition topicPartition,
            UUID uuid,
            Instant instant
    ) {
      this.storageURL = storageURL;
      this.logsDir = logsDir;
      this.topicPartition = topicPartition;
      this.uuid = uuid;
      this.instant = instant;
    }

    public String filePath() {
      return FileUtils.fileName(
              storageURL,
              logsDir,
              topicPartition,
              String.format("%s-%s.%s", uuid, instant.toEpochMilli(), LOCK_FILE_EXTENSION)
      );
    }
  }

  private Lock getNewLock() {
    return new Lock(
            this.storage.url(),
            this.logsDir,
            this.topicPartition,
            this.uuid,
            Instant.now()
    );
  }

  private Lock createOrRenameLock() {
    if (!this.storage.exists(this.lockDir)) {
      this.storage.create(this.lockDir);
    }

    // It gives two chance of grabbing the lock on startup.
    // This should help when the new task comes up sooner than the expiry of the previous lock.
    // Wait for lockTimeout + lockRefreshInterval to give enough time to release the lock
    if (!findAliveLocks().isEmpty()) {
      try {
        Thread.sleep(this.lockTimeout.toMillis() + this.lockRefreshInterval.toMillis());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (!findAliveLocks().isEmpty()) {
        throw new ConnectException("Lock has been acquired by another process");
      }
    }

    Lock newLock = getNewLock();
    this.storage.create(newLock.filePath(), true);

    if (findAliveLocks().size() > 1) {
      // This kills the current task and gives the chance to
      // the other process to consume.
      // If both start to consume, at least one will die when it acquires the lock.
      this.storage.delete(newLock.filePath());
      throw new ConnectException("Lock has been acquired by another process");
    }

    return newLock;
  }


  private void startRenamingTimer() {
    this.timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        renameLockFile();
      }
    }, this.lockRefreshInterval.toMillis(), this.lockRefreshInterval.toMillis());
  }

  private void renameLockFile() {
    Lock newLock = getNewLock();

    try {
      this.storage.commit(this.lock.filePath(), newLock.filePath());
      this.lock = newLock;
    } catch (Exception e) {
      log.error("Failed to rename the file", e);
    }
  }

  private List<Lock> findLocks() {
    return this.storage.list(this.lockDir)
            .stream()
            .filter(FileStatus::isFile)
            .map(FileStatus::getPath)
            .map(Path::getName)
            .map(pattern::matcher)
            .filter(Matcher::matches)
            .map(m ->
              new Lock(
                this.storage.url(),
                this.logsDir,
                this.topicPartition,
                UUID.fromString(m.group("uuid")),
                Instant.ofEpochMilli(Long.parseLong(m.group("epochInMS")))
            )).collect(Collectors.toList());
  }

  private List<Lock> findAliveLocks() {
    return this.findLocks()
            .stream()
            .filter(l -> l.instant.isAfter(Instant.now().minus(this.lockTimeout)))
            .collect(Collectors.toList());
  }

  @Override
  public void acquireLease() throws ConnectException {
    // The ownership of lock is determined by the lock filenames.
    // If there are more than two locks or the current process is not the owner,
    // this method throws a ConnectException which kills the task.
    List<Lock> aliveLocks = this.findAliveLocks();

    if (aliveLocks.isEmpty()) {
      throw new ConnectException("The lock file is not present in the log dir");
    }

    if (aliveLocks.size() > 1) {
      throw new ConnectException("More than one alive lock file reside in the log dir");
    }

    Lock aliveLock = aliveLocks.get(0);

    if (!aliveLock.uuid.equals(this.uuid)) {
      log.error(
              "UUID of the lock file {} for {}-{} topic-partition does not match {} uuid",
              aliveLock.filePath(),
              this.topicPartition.topic(),
              this.topicPartition.partition(),
              this.uuid);
      throw new ConnectException("Lock uuid does not match");
    }

    if (aliveLock.instant.isBefore(Instant.now().minus(this.lockTimeout))) {
      throw new ConnectException(
              "Lock file has not been renamed for more than the threshold");
    }
  }

  @Override
  public void append(String s, String s1) throws ConnectException {
    this.acquireLease();
  }

  @Override
  public void apply() throws ConnectException {
  }

  @Override
  public void truncate() throws ConnectException {
  }

  @Override
  public void close() throws ConnectException {
    this.timer.cancel();
    this.storage.delete(this.lock.filePath());
  }

  @Override
  public String getLogFile() {
    return null;
  }

  @Override
  public FilePathOffset extractLatestOffset() {
    // QFSWAL doesn't keep the latest-offset in the generated files,
    // returning null makes TopicPartitioner fall back to read offsets from the file names.
    return null;
  }
}
