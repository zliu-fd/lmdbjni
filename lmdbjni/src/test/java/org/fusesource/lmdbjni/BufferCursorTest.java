package org.fusesource.lmdbjni;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
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
    db = env.openDatabase("test", Constants.DUPSORT | Constants.CREATE);
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
  public void testBufferCursor() {
    Transaction tx = env.createReadTransaction();
    try (BufferCursor cursor = db.bufferCursor(tx)) {
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
    tx.commit();
  }

  @Test
  public void testDelete() {
    Transaction tx = env.createWriteTransaction();
    try (BufferCursor cursor = db.bufferCursor(tx)) {
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
    tx.commit();
  }

  @Test
  public void testOverwrite() {
    Transaction tx = env.createWriteTransaction();
    try (BufferCursor cursor = db.bufferCursor(tx)) {
      assertTrue(cursor.seek(Bytes.fromLong(0)));
      cursor.delete();
      assertTrue(cursor.keyWriteLong(0).valWriteByte(100).overwrite());
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valByte(0), is((byte) 100));
      assertTrue(cursor.valWriteByte(200).overwrite());
      assertTrue(cursor.first());
      assertTrue(cursor.nextDup());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valByte(0), is((byte) 200));
    }
    tx.commit();
    tx = env.createWriteTransaction();
    try (BufferCursor cursor = db.bufferCursor(tx)) {
      assertTrue(cursor.first());
      assertTrue(cursor.nextDup());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valByte(0), is((byte) 200));
    }
    tx.commit();
  }

  @Test
  public void testAppend() {
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.keyWriteByte(100).valWriteByte(100).append();
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
      assertTrue(cursor.last());
      assertThat(cursor.keyByte(0), is((byte) 100L));
      assertThat(cursor.valByte(0), is((byte) 100L));
      tx.commit();
    }

    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
      assertTrue(cursor.last());
      assertThat(cursor.keyByte(0), is((byte) 100));
      assertThat(cursor.valByte(0), is((byte) 100L));
      tx.commit();
    }
  }

  @Test
  public void testDirectBuffer() {
    DirectBuffer key = new DirectBuffer(ByteBuffer.allocateDirect(10));
    DirectBuffer value = new DirectBuffer(ByteBuffer.allocateDirect(10));
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx, key, value)) {
      cursor.setWriteMode();
      key.putString(0, new ByteString("a"));
      value.putString(0, new ByteString("a"));
      cursor.put();
      cursor.last();
      assertThat(cursor.keyUtf8(0).getString(), is("a"));
      assertThat(cursor.valUtf8(0).getString(), is("a"));
      tx.commit();
    }
    key = new DirectBuffer(ByteBuffer.allocateDirect(10));
    value = new DirectBuffer(ByteBuffer.allocateDirect(10));
    try (Transaction tx = env.createReadTransaction(); BufferCursor cursor = db.bufferCursor(tx, key, value)) {
      cursor.last();
      assertThat(cursor.keyUtf8(0).getString(), is("a"));
      assertThat(cursor.valUtf8(0).getString(), is("a"));
    }
  }


  @Test
  public void testPut() {
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      assertTrue(cursor.seek(new byte[]{1}));
      assertFalse(cursor
        .keyWriteByte(1)
        .valWriteByte(100)
        .put());
      assertTrue(cursor.seek(new byte[]{1}));
      assertTrue(cursor.first());
      assertThat(cursor.keyLong(0), is(0L));
      assertThat(cursor.valLong(0), is(0L));
      assertTrue(cursor
        .keyWriteByte(111)
        .valWriteByte(121)
        .put());
      assertTrue(cursor.seek(new byte[]{111}));
      assertThat(cursor.keyByte(0), is((byte) 111));
      assertThat(cursor.valByte(0), is((byte) 121));
      tx.commit();
    }
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      assertTrue(cursor.seek(new byte[]{111}));
      assertThat(cursor.keyByte(0), is((byte) 111));
      assertThat(cursor.valByte(0), is((byte) 121));
      tx.commit();
    }
  }

  @Test
  public void testDup() {
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.keyWriteUtf8("abc").valWriteUtf8("1")
        .overwrite();
      cursor.keyWriteUtf8("abc").valWriteUtf8("2")
        .overwrite();
      cursor.keyWriteUtf8("abc").valWriteUtf8("3")
        .overwrite();
      assertTrue(cursor.last());
      assertThat(cursor.keyUtf8(0).getString(), is("abc"));
      assertThat(cursor.valUtf8(0).getString(), is("3"));
      assertTrue(cursor.prevDup());
      assertThat(cursor.keyUtf8(0).getString(), is("abc"));
      assertThat(cursor.valUtf8(0).getString(), is("2"));
      assertTrue(cursor.prevDup());
      assertThat(cursor.keyUtf8(0).getString(), is("abc"));
      assertThat(cursor.valUtf8(0).getString(), is("1"));
      assertFalse(cursor.prevDup());
      assertTrue(cursor.lastDup());
      assertThat(cursor.keyUtf8(0).getString(), is("abc"));
      assertThat(cursor.valUtf8(0).getString(), is("3"));
      assertTrue(cursor.firstDup());
      assertTrue(cursor.firstDup());
      assertThat(cursor.keyUtf8(0).getString(), is("abc"));
      assertThat(cursor.valUtf8(0).getString(), is("1"));
      tx.commit();
    }
  }

  @Test
  public void testWriteDataTypes() {
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.first();
      cursor.keyWriteByte(111)
        .keyWriteInt(1)
        .keyWriteLong(2)
        .keyWriteFloat(1.0f)
        .keyWriteDouble(2.0)
        .keyWriteBytes(new byte[]{1, 2, 3})
        .keyWriteUtf8(new ByteString("abc"))
        .valWriteByte(112)
        .valWriteInt(3)
        .valWriteLong(4)
        .valWriteFloat(5.0f)
        .valWriteDouble(6.0)
        .valWriteBytes(new byte[]{1, 2, 3})
        .valWriteUtf8("cba")
        .overwrite();
      tx.commit();
    }

    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.last();
      assertThat(cursor.keyByte(0), is((byte)111));
      assertThat(cursor.keyInt(1), is(1));
      assertThat(cursor.keyLong(5), is(2L));
      assertThat(cursor.keyFloat(13), is(1.0f));
      assertThat(cursor.keyDouble(17), is(2.0));
      assertArrayEquals(cursor.keyBytes(25, 3), new byte[]{1,2,3});
      assertThat(cursor.keyUtf8(28).getString(), is("abc"));
      assertThat(cursor.keyDirectBuffer().getDouble(17, ByteOrder.BIG_ENDIAN), is(2.0d));
      assertThat(cursor.valByte(0), is((byte)112));
      assertThat(cursor.valInt(1), is(3));
      assertThat(cursor.valLong(5), is(4L));
      assertThat(cursor.valFloat(13), is(5.0f));
      assertThat(cursor.valDouble(17), is(6.0));
      assertArrayEquals(cursor.valBytes(25, 3), new byte[]{1,2,3});
      assertThat(cursor.valUtf8(28), is(new ByteString("cba")));
      assertThat(cursor.valDirectBuffer().getDouble(17, ByteOrder.BIG_ENDIAN), is(6.0d));
      tx.commit();
    }
  }

  @Test
  public void testByteString() {
    ByteString string = new ByteString("cba");
    assertThat(string.length(), is(3));
    HashMap<ByteString, ByteString> map = new HashMap<>();
    map.put(string, string);
    assertThat(string, is(new ByteString("cba")));
  }


  @Test
  public void testWriteStrings() {
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.first();
      cursor.keyWriteUtf8("abc")
        .keyWriteUtf8("def")
        .valWriteUtf8("ghi")
        .valWriteUtf8(new ByteString("jkl"))
        .overwrite();
      tx.commit();
    }
    try (Transaction tx = env.createReadTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.last();
      ByteString string = cursor.keyUtf8(0);
      assertThat(string.getString(), is("abc"));
      // add NULL byte
      string = cursor.keyUtf8(string.size() + 1);
      assertThat(string.getString(), is("def"));
      string = cursor.valUtf8(0);
      assertThat(string.getString(), is("ghi"));
      // add NULL byte
      string = cursor.valUtf8(string.size() + 1);
      assertThat(string, is(new ByteString("jkl")));
    }
  }

  @Test
  public void testWriteBuffers() {
    ByteString stringKey = new ByteString("key");
    ByteString stringValue = new ByteString("value");
    try (Transaction tx = env.createWriteTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.first();
      DirectBuffer key = new DirectBuffer();
      key.putString(0, stringKey);
      // remember NULL byte
      cursor.keyWrite(key, stringKey.size() + 1);
      DirectBuffer value = new DirectBuffer();
      value.putString(0, stringValue);
      // remember NULL byte
      cursor.valWrite(value, stringValue.size() + 1);
      cursor.put();
      debug(cursor);
      tx.commit();
    }
    try (Transaction tx = env.createReadTransaction(); BufferCursor cursor = db.bufferCursor(tx)) {
      cursor.last();
      ByteString string = cursor.keyUtf8(0);
      assertThat(string.getString(), is("key"));
      string = cursor.valUtf8(0);
      assertThat(string.getString(), is("value"));
    }
  }

  @Test
  public void testWriteToReadOnlyBuffer() {
    try (Transaction tx = env.createReadTransaction(); final BufferCursor cursor = db.bufferCursor(tx)) {
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteByte(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteInt(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteLong(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteFloat(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteDouble(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteUtf8(""); }});
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteUtf8(new ByteString("")); }});
        assertEACCES(new Runnable() { public void run() { cursor.keyWriteBytes(new byte[]{0});}});
        assertEACCES(new Runnable() { public void run() { cursor.keyWrite(new DirectBuffer(new byte[0]), 0);}});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteByte(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteInt(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteLong(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteFloat(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteDouble(0); }});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteUtf8(""); }});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteUtf8(new ByteString("")); }});
        assertEACCES(new Runnable() { public void run() { cursor.valWriteBytes(new byte[]{0});}});
        assertEACCES(new Runnable() { public void run() { cursor.valWrite(new DirectBuffer(new byte[0]), 0);}});
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

  private void assertEACCES(Runnable runnable) {
    try {
      runnable.run();
      fail("should throw EACCES");
    } catch (LMDBException e) {
      assertThat(e.getErrorCode(), is(LMDBException.EACCES));
    }
  }
}
