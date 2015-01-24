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
    try (EntryIterator it = db.iterate()) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollFirst(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
  }

  @Test
  public void testIterateBackward() throws IOException {
    try (EntryIterator it = db.iterateBackward()) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollLast(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
  }

  @Test
  public void testSeekForward() throws IOException {
    keys.pollFirst();
    keys.pollFirst();
    keys.pollFirst();
    keys.pollFirst();
    keys.pollFirst();
    try (EntryIterator it = db.seek(new byte[]{5})) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollFirst(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
  }

  @Test
  public void testSeekBackward() throws IOException {
    keys.pollLast();
    keys.pollLast();
    keys.pollLast();
    keys.pollLast();
    try (EntryIterator it = db.seekBackward(new byte[]{5})) {
      while (it.hasNext()) {
        Entry next = it.next();
        assertArrayEquals(keys.pollLast(), next.getKey());
      }
      assertTrue(keys.isEmpty());
    }
  }

  @Test
  public void testIterable() throws IOException {
    try (EntryIterator it = db.iterate()) {
      for (Entry next : it.iterable()) {
        assertArrayEquals(keys.pollFirst(), next.getKey());
      }
    }
    assertTrue(keys.isEmpty());
  }
}
