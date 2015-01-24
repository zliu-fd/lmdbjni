package org.fusesource.lmdbjni;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Do not use BufferCursor on Android.
 */
public class BufferCursor implements Closeable {
  private final Cursor cursor;
  private final Transaction tx;
  private final DirectBuffer key;
  private final DirectBuffer value;
  private final ByteBuffer byteBuffer;
  private boolean lastSeek = false;

  BufferCursor(Cursor cursor, Transaction tx, DirectBuffer key, DirectBuffer value) {
    this.cursor = cursor;
    this.tx = tx;
    this.key = key;
    this.value = value;
    this.byteBuffer = key.byteBuffer();
    if (byteBuffer == null) {
      throw new IllegalArgumentException("No ByteBuffer available for key.");
    }
    if (!byteBuffer.isDirect()) {
      throw new IllegalArgumentException("ByteBuffer for key must be direct.");
    }
  }

  public boolean seek(byte[] key) {
    ByteBuffer buf = ByteBuffer.allocateDirect(key.length);
    this.key.wrap(buf);
    this.key.putBytes(0, key);
    lastSeek = true;
    return cursor.seekPosition(this.key, value, SeekOp.RANGE) == 0;
  }

  public boolean first() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.FIRST) == 0;
  }

  public boolean last() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.LAST) == 0;
  }

  public boolean next() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.NEXT) == 0;
  }

  public boolean prev() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.PREV) == 0;
  }

  @Override
  public void close() throws IOException {
    if (tx != null) {
      tx.commit();
    }
    cursor.close();
  }
}
