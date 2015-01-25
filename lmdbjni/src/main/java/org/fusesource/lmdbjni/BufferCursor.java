package org.fusesource.lmdbjni;

import java.nio.ByteBuffer;

/**
 * A cursor that allow for zero-copy lookup and navigation by using
 * addresses provided by LMDB instead of copying data for each operation.
 * Do not use on Android.
 *
 * <pre>
 * {@code
 * DirectBuffer k = new DirectBuffer();
 * DirectBuffer v = new DirectBuffer();
 *
 * try (BufferCursor cursor = db.bufferCursor(key, value)) {
 *   cursor.first();
 *   while(cursor.next()) {
 *     k.getByte(0);
 *     v.getByte(0);
 *   }
 *
 *    cursor.last();
 *    while(cursor.prev()) {
 *      k.getByte(0);
 *      v.getByte(0);
 *    }
 *
 *    cursor.seek(bytes("London"));
 *    k.getByte(0);
 *    v.getByte(0);
 *  }
 * }
 * </pre>
 *
 * @author Kristoffer Sjögren
 */
public class BufferCursor implements AutoCloseable {
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

  /**
   * Position at first key greater than or equal to specified key.
   *
   * @param key key to seek for.
   * @return true if a key was found.
   */
  public boolean seek(byte[] key) {
    ByteBuffer buf = ByteBuffer.allocateDirect(key.length);
    this.key.wrap(buf);
    this.key.putBytes(0, key);
    lastSeek = true;
    return cursor.seekPosition(this.key, value, SeekOp.RANGE) == 0;
  }

  /**
   * Position at first key/data item.
   *
   * @return true if found
   */
  public boolean first() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.FIRST) == 0;
  }

  /**
   * Position at last key/data item.
   *
   * @return true if found
   */
  public boolean last() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.LAST) == 0;
  }

  /**
   * Position at next data item.
   *
   * @return true if found
   */
  public boolean next() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.NEXT) == 0;
  }

  /**
   * Position at next data item of current key.
   * Only for #MDB_DUPSORT
   *
   * @return true if found
   */
  public boolean nextDup() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.NEXT_DUP) == 0;
  }


  /**
   * Position at previous data item.
   *
   * @return true if found
   */
  public boolean prev() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.PREV) == 0;
  }

  /**
   * Position at previous data item of current key.
   * Only for #MDB_DUPSORT
   *
   * @return true if found
   */
  public boolean prevDup() {
    if (lastSeek) {
      key.wrap(byteBuffer);
      lastSeek = false;
    }
    return cursor.position(key, value, GetOp.PREV_DUP) == 0;
  }

  /**
   * <p>
   *   Delete key/data pair at current cursor position.
   * </p>
   *
   */
  public void delete() {
    cursor.delete();
  }

  public void put() {
    cursor.put(key, value, 0);
  }

  /**
   * Close the cursor and the transaction.
   */
  @Override
  public void close() {
    if (tx != null) {
      cursor.close();
      tx.commit();
    } else {
      cursor.close();
    }
  }
}
