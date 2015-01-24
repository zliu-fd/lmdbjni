package org.fusesource.lmdbjni;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
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
  DirectBuffer key = new DirectBuffer(ByteBuffer.allocateDirect(8));
  DirectBuffer value = new DirectBuffer(0, 0);

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
  public void testBufferCursor() throws IOException {
    try (BufferCursor cursor = db.bufferCursor(key, value)) {
      assertTrue(cursor.first());
      assertThat(key.getByte(0), is((byte) 0));
      assertTrue(cursor.next());
      assertThat(key.getByte(0), is((byte) 1));
      assertTrue(cursor.prev());
      assertThat(key.getByte(0), is((byte) 0));
      // go to far
      assertFalse(cursor.prev());
      assertFalse(cursor.prev());
      assertThat(key.getByte(0), is((byte) 0));

      assertTrue(cursor.last());
      assertThat(key.getByte(0), is((byte) 9));
      assertTrue(cursor.prev());
      assertThat(key.getByte(0), is((byte) 8));
      // go too far
      assertTrue(cursor.next());
      assertFalse(cursor.next());
      assertThat(key.getByte(0), is((byte) 9));

      cursor.first();
      int expected = 1;
      while(cursor.next()) {
        assertThat(key.getByte(0), is((byte) expected++));
      }

      cursor.last();
      expected = 8;
      while(cursor.prev()) {
        assertThat(key.getByte(0), is((byte) expected--));
      }
    }
  }
}
