package org.fusesource.lmdbjni;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class BufferCursorTest {
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
    for (int i = 1; i < 10; i++) {
      byte[] bytes = new byte[]{(byte) i};
      keys.add(bytes);
      db.put(bytes, bytes);
    }

    for (int i = 0; i < 10; i++) {
      byte[] bytes = Bytes.fromLong(i);
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
  public void testBufferCursor() throws IOException {
    try (BufferCursor cursor = db.bufferCursor()) {
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      // go to far
      assertFalse(cursor.prev());
      assertFalse(cursor.prev());
      assertThat(cursor.keyGetLong(0), is(0L));

      assertTrue(cursor.seek(new byte[]{1}));
      assertThat(cursor.keyGetByte(0), is((byte) 1));

      assertTrue(cursor.seek(Bytes.fromLong(1)));
      assertThat(cursor.keyGetLong(0), is(1L));

      assertTrue(cursor.seek(new byte[]{1}));
      assertThat(cursor.keyGetByte(0), is((byte) 1));
      assertTrue(cursor.next());
      assertThat(cursor.keyGetByte(0), is((byte) 2));
      assertTrue(cursor.prev());
      assertThat(cursor.keyGetByte(0), is((byte) 1));

      assertTrue(cursor.last());
      assertThat(cursor.keyGetByte(0), is((byte) 9));
      assertTrue(cursor.prev());
      assertThat(cursor.keyGetByte(0), is((byte) 8));
      // go too far
      assertTrue(cursor.next());
      assertFalse(cursor.next());
      assertThat(cursor.keyGetByte(0), is((byte) 9));

      assertTrue(cursor.seek(new byte[]{5}));
      assertThat(cursor.keyGetByte(0), is((byte) 5));
      assertThat(cursor.valGetByte(0), is((byte) 5));


      assertTrue(cursor.first());
      long expected = 1;
      while (cursor.next()) {
        if (expected > 9) {
          break;
        }
        assertThat(cursor.keyGetLong(0), is(expected++));
      }

      assertTrue(cursor.last());
      expected = 8;
      while (cursor.prev()) {
        if (expected < 1) {
          break;
        }
        assertThat(cursor.keyGetByte(0), is((byte) expected--));
      }
    }
  }

  @Test
  public void testDelete() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetLong(0), is(0L));
      cursor.next();
      assertThat(cursor.keyGetLong(0), is(1L));
      assertThat(cursor.valGetLong(0), is(1L));
      cursor.delete();
      // the buffer still holds the value just deleted
      assertThat(cursor.keyGetLong(0), is(1L));
      assertThat(cursor.valGetLong(0), is(1L));
      cursor.next();
      assertThat(cursor.keyGetLong(0), is(2L));
      assertThat(cursor.valGetLong(0), is(2L));
      cursor.prev();
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetLong(0), is(0L));
    }
  }

  @Test
  public void testOverwrite() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.seek(Bytes.fromLong(0)));
      cursor.valWriteByte(100);
      assertTrue(cursor.overwrite());
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetByte(0), is((byte) 100));
      cursor.valWriteByte(200);
      assertTrue(cursor.overwrite());
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetByte(0), is((byte) 200));
    }
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetByte(0), is((byte) 200));
    }
  }

  @Test
  public void testAppend() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      cursor.keyWriteByte(100).valWriteByte(100);
      cursor.append();
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetLong(0), is(0L));
      assertTrue(cursor.last());
      assertThat(cursor.keyGetByte(0), is((byte) 100L));
      assertThat(cursor.valGetByte(0), is((byte) 100L));

    }

    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetLong(0), is(0L));
      assertTrue(cursor.last());
      assertThat(cursor.keyGetByte(0), is((byte) 100));
      assertThat(cursor.valGetByte(0), is((byte) 100L));
    }
  }

  @Test
  public void testPut() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.seek(new byte[]{1}));
      cursor.keyWriteByte(1).valWriteByte(100);
      assertFalse(cursor.put());
      assertTrue(cursor.seek(new byte[]{1}));
      assertTrue(cursor.first());
      assertThat(cursor.keyGetLong(0), is(0L));
      assertThat(cursor.valGetLong(0), is(0L));
      cursor.keyWriteByte(111).valWriteByte(121);
      assertTrue(cursor.put());
      assertTrue(cursor.seek(new byte[]{111}));
      assertThat(cursor.keyGetByte(0), is((byte) 111));
      assertThat(cursor.valGetByte(0), is((byte) 121));
    }

    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.seek(new byte[]{111}));
      assertThat(cursor.keyGetByte(0), is((byte) 111));
      assertThat(cursor.valGetByte(0), is((byte) 121));
    }
  }

  private void debug(BufferCursor cursor) {
    System.out.println("----");
    cursor.first();
    System.out.println(Arrays.toString(cursor.keyBytes()) + " " + Arrays.toString(cursor.valBytes()));
    while (cursor.next()) {
      System.out.println(Arrays.toString(cursor.keyBytes()) + " " + Arrays.toString(cursor.valBytes()));
    }
    System.out.println("----");
  }
}
