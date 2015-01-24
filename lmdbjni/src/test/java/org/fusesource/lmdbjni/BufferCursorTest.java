package org.fusesource.lmdbjni;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

  DirectBuffer key;
  DirectBuffer value;

  @Before
  public void before() throws IOException {
    key = new DirectBuffer();
    value = new DirectBuffer();
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
    try (BufferCursor cursor = db.bufferCursor(key, value)) {
      assertTrue(cursor.first());
      assertThat(key.getLong(0, ByteOrder.BIG_ENDIAN), is(0L));
      // go to far
      assertFalse(cursor.prev());
      assertFalse(cursor.prev());
      assertThat(key.getLong(0, ByteOrder.BIG_ENDIAN), is(0L));

      assertTrue(cursor.seek(new byte[]{1}));
      assertThat(key.getByte(0), is((byte) 1));

      assertTrue(cursor.seek(Bytes.fromLong(1)));
      assertThat(key.getLong(0, ByteOrder.BIG_ENDIAN), is(1L));

      assertTrue(cursor.seek(new byte[]{1}));
      assertThat(key.getByte(0), is((byte) 1));
      assertTrue(cursor.next());
      assertThat(key.getByte(0), is((byte) 2));
      assertTrue(cursor.prev());
      assertThat(key.getByte(0), is((byte) 1));

      assertTrue(cursor.last());
      assertThat(key.getByte(0), is((byte) 9));
      assertTrue(cursor.prev());
      assertThat(key.getByte(0), is((byte) 8));
      // go too far
      assertTrue(cursor.next());
      assertFalse(cursor.next());
      assertThat(key.getByte(0), is((byte) 9));

      assertTrue(cursor.seek(new byte[]{5}));
      assertThat(key.getByte(0), is((byte) 5));
      assertThat(value.getByte(0), is((byte) 5));


      assertTrue(cursor.first());
      long expected = 1;
      while(cursor.next()) {
        if (expected > 9) {
          break;
        }
        assertThat(key.getLong(0, ByteOrder.BIG_ENDIAN), is(expected++));
      }

      assertTrue(cursor.last());
      expected = 8;
      while(cursor.prev()) {
        if (expected < 1) {
          break;
        }
        assertThat(key.getByte(0), is((byte) expected--));
      }
    }
  }

  @Test
  public void testTooSmallBuffers() {
    byte[] key1 = "key1".getBytes();
    byte[] val1 = "val1".getBytes();
    db.put(key1, val1);
    byte[] key2 = "key2".getBytes();
    byte[] val2 = "val2".getBytes();
    db.put(key2, val2);
    byte[] bytes = new byte[key1.length];
    key = new DirectBuffer(ByteBuffer.allocateDirect(1));
    value = new DirectBuffer(ByteBuffer.allocateDirect(1));
    try (BufferCursor cursor = db.bufferCursor(key, value)) {
      assertTrue(cursor.seek(key1));
      key.getBytes(0, bytes);
      assertThat(new String(bytes), is("key1"));
      value.getBytes(0, bytes);
      assertThat(new String(bytes), is("val1"));

      assertTrue(cursor.seek(new byte[] {10}));
      assertTrue(cursor.next());
      key.getBytes(0, bytes);
      assertThat(new String(bytes), is("key2"));
      value.getBytes(0, bytes);
      assertThat(new String(bytes), is("val2"));

      assertTrue(cursor.prev());
      key.getBytes(0, bytes);
      assertThat(new String(bytes), is("key1"));
      value.getBytes(0, bytes);
      assertThat(new String(bytes), is("val1"));
    }
}
}
