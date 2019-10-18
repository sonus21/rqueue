package com.github.sonus21.rqueue.core;

import java.util.concurrent.TimeUnit;

public class LockManager {
  private StringMessageTemplate stringMessageTemplate;
  private static final long LOCK_EXPIRY_TIME = 10;

  public LockManager(StringMessageTemplate stringMessageTemplate) {
    this.stringMessageTemplate = stringMessageTemplate;
  }

  public boolean acquireLock(String lock) {
    try {
      return stringMessageTemplate.putIfAbsent(lock, LOCK_EXPIRY_TIME, TimeUnit.SECONDS);
    } catch (Exception e) {
      return false;
    }
  }

  public void releaseLock(String lock) {
    stringMessageTemplate.delete(lock);
  }
}
