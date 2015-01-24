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
  BufferCursor(Cursor cursor, Transaction tx, DirectBuffer key, DirectBuffer value) {
    this.cursor = cursor;
    this.tx = tx;
    this.key = key;
    this.value = value;
    this.byteBuffer = key.byteBuffer();
    if (byteBuffer == null) {
      throw new IllegalArgumentException("No ByteBuffer available for key.");
    }
  }

  public boolean seek(byte[] key) {
    this.key.wrap(byteBuffer);
    this.key.putBytes(0, key);
    return cursor.seekPosition(this.key, value, SeekOp.RANGE) == 0;
  }

  public boolean first() {
    return cursor.position(key, value, GetOp.FIRST) == 0;
  }

  public boolean last() {
    return cursor.position(key, value, GetOp.LAST) == 0;
  }

  public boolean next() {
    return cursor.position(key, value, GetOp.NEXT) == 0;
  }

  public boolean prev() {
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
