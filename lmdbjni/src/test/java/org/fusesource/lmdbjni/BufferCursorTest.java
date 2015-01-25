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
      assertThat(cursor.keyLong(0), is(0L));
      // go to far
      assertFalse(cursor.prev());
      assertFalse(cursor.prev());
      assertThat(cursor.keyLong(0), is(0L));

      assertTrue(cursor.seek(new byte[]{1}));
      assertThat(cursor.keyByte(0), is((byte) 1));

      assertTrue(cursor.seek(Bytes.fromLong(1)));
      assertThat(cursor.keyLong(0), is(1L));

      assertTrue(cursor.seek(new byte[]{1}));
      assertThat(cursor.keyByte(0), is((byte) 1));
      assertTrue(cursor.next());
      assertThat(cursor.keyByte(0), is((byte) 2));
      assertTrue(cursor.prev());
      assertThat(cursor.keyByte(0), is((byte) 1));

      assertTrue(cursor.last());
      assertThat(cursor.keyByte(0), is((byte) 9));
      assertTrue(cursor.prev());
      assertThat(cursor.keyByte(0), is((byte) 8));
      // go too far
      assertTrue(cursor.next());
      assertFalse(cursor.next());
      assertThat(cursor.keyByte(0), is((byte) 9));

      assertTrue(cursor.seek(new byte[]{5}));
      assertThat(cursor.keyByte(0), is((byte) 5));
      assertThat(cursor.valByte(0), is((byte) 5));


      assertTrue(cursor.first());
      long expected = 1;
      while (cursor.next()) {
        if (expected > 9) {
          break;
        }
        assertThat(cursor.keyLong(0), is(expected++));
      }

      assertTrue(cursor.last());
      expected = 8;
      while (cursor.prev()) {
        if (expected < 1) {
          break;
        }
        assertThat(cursor.keyByte(0), is((byte) expected--));
      }
    }
  }

  @Test
  public void testDelete() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
      cursor.next();
      assertThat(cursor.keyLong(0), is(1L));
      assertThat(cursor.valLong(0), is(1L));
      cursor.delete();
      // the buffer still holds the value just deleted
      assertThat(cursor.keyLong(0), is(1L));
      assertThat(cursor.valLong(0), is(1L));
      cursor.next();
      assertThat(cursor.keyLong(0), is(2L));
      assertThat(cursor.valLong(0), is(2L));
      cursor.prev();
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
    }
  }

  @Test
  public void testOverwrite() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.seek(Bytes.fromLong(0)));
      cursor.valWriteByte(100);
      assertTrue(cursor.overwrite());
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valByte(0), is((byte) 100));
      cursor.valWriteByte(200);
      assertTrue(cursor.overwrite());
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valByte(0), is((byte) 200));
    }
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valByte(0), is((byte) 200));
    }
  }

  @Test
  public void testAppend() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      cursor.keyWriteByte(100).valWriteByte(100);
      cursor.append();
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
      assertTrue(cursor.last());
      assertThat(cursor.keyByte(0), is((byte) 100L));
      assertThat(cursor.valByte(0), is((byte) 100L));

    }

    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
      assertTrue(cursor.last());
      assertThat(cursor.keyByte(0), is((byte) 100));
      assertThat(cursor.valByte(0), is((byte) 100L));
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
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
      cursor.keyWriteByte(111).valWriteByte(121);
      assertTrue(cursor.put());
      assertTrue(cursor.seek(new byte[]{111}));
      assertThat(cursor.keyByte(0), is((byte) 111));
      assertThat(cursor.valByte(0), is((byte) 121));
    }

    try (BufferCursor cursor = db.bufferCursorWriter()) {
      assertTrue(cursor.seek(new byte[]{111}));
      assertThat(cursor.keyByte(0), is((byte) 111));
      assertThat(cursor.valByte(0), is((byte) 121));
    }
  }

  @Test
  public void testWriteDataTypes() {
    try (BufferCursor cursor = db.bufferCursorWriter()) {
      cursor.first();
      cursor.keyWriteByte(111)
        .keyWriteInt(1)
        .keyWriteLong(2)
        .keyWriteFloat(1.0f)
        .keyWriteDouble(2.0)
        .keyWriteBytes(new byte[]{1, 2, 3})
        .keyWriteUtf8("abc")
        .valWriteByte(112)
        .valWriteInt(3)
        .valWriteLong(4)
        .valWriteFloat(5.0f)
        .valWriteDouble(6.0)
        .valWriteBytes(new byte[]{1, 2, 3})
        .valWriteUtf8("cba");
      cursor.overwrite();
    }

    try (BufferCursor cursor = db.bufferCursorWriter()) {
      cursor.last();
      assertThat(cursor.keyByte(0), is((byte)111));
      assertThat(cursor.keyInt(1), is(1));
      assertThat(cursor.keyLong(5), is(2L));
      assertThat(cursor.keyFloat(13), is(1.0f));
      assertThat(cursor.keyDouble(17), is(2.0));
      assertArrayEquals(cursor.keyBytes(25, 3), new byte[]{1,2,3});
      assertThat(cursor.keyUtf8(28).getString(), is("abc"));
      assertThat(cursor.valByte(0), is((byte)112));
      assertThat(cursor.valInt(1), is(3));
      assertThat(cursor.valLong(5), is(4L));
      assertThat(cursor.valFloat(13), is(5.0f));
      assertThat(cursor.valDouble(17), is(6.0));
      assertArrayEquals(cursor.valBytes(25, 3), new byte[]{1,2,3});
      assertThat(cursor.valUtf8(28).getString(), is("cba"));
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
