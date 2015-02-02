package org.fusesource.lmdbjni;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.LinkedList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class EntryIteratorTest {
  static {
    Setup.setLmdbLibraryPath();
  }

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  Env env;
  Database db;
  LinkedList<byte[]> keys;

  @Before
  public void before() throws IOException {
    String path = tmp.newFolder().getCanonicalPath();
    env = new Env(path);
    db = env.openDatabase();
    keys = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      byte[] bytes = new byte[]{(byte) i};
      keys.add(bytes);
      db.put(bytes, bytes);
    }
  }

  @After
  public void after() {
    db.close();
    env.close();
  }

  @Test
  public void testIterateForward() throws IOException {
    Transaction tx = env.createReadTransaction();
    try (EntryIterator it = db.iterate(tx)) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollFirst(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
    tx.commit();
  }

  @Test
  public void testIterateBackward() throws IOException {
    Transaction tx = env.createReadTransaction();
    try (EntryIterator it = db.iterateBackward(tx)) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollLast(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
    tx.commit();
  }

  @Test
  public void testSeekForward() throws IOException {
    keys.pollFirst();
    keys.pollFirst();
    keys.pollFirst();
    keys.pollFirst();
    keys.pollFirst();
    Transaction tx = env.createReadTransaction();
    try (EntryIterator it = db.seek(tx, new byte[]{5})) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollFirst(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
    tx.commit();
  }

  @Test
  public void testSeekBackward() throws IOException {
    keys.pollLast();
    keys.pollLast();
    keys.pollLast();
    keys.pollLast();
    Transaction tx = env.createReadTransaction();
    try (EntryIterator it = db.seekBackward(tx, new byte[]{5})) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollLast(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
    tx.commit();
  }

  @Test
  public void testIterable() throws IOException {
    Transaction tx = env.createReadTransaction();
    try (EntryIterator it = db.iterate(tx)) {
      for (Entry next : it.iterable()) {
        assertArrayEquals(keys.pollFirst(), next.getKey());
      }
    }
    assertTrue(keys.isEmpty());
    tx.commit();
  }
}
