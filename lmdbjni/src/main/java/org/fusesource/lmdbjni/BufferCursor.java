package org.fusesource.lmdbjni;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Cursor mode that allow for zero-copy lookup, navigation and modification
 * by using addresses provided by LMDB instead of copying data for each
 * operation.
 * <p/>
 * <p>
 * This is an advanced mode. Users should avoid interacting directly
 * with DirectBuffer and use the BufferCursor API instead. Otherwise
 * take extra care of buffer memory address+size and byte ordering.
 * Mistakes may lead to SIGSEGV or unpredictable key ordering etc.
 * </p>
 * <p/>
 * <p>
 * Key and value buffers are updated as the cursor moves. After a
 * cursor have been moved, the buffers will point to a memory address
 * owned by the database. The caller need not dispose of the memory,
 * and may not modify it in any way. Modification to buffers in this
 * state will cause a SIGSEGV.
 * </p>
 * <p/>
 * <p>
 * Any modification will be written into cached byte buffers which needs
 * to be sized in order to fit key and value data written into them.
 * The key buffer is 511 by default which is the max key size in LMDB.
 * This limitation will be fixed in later versions to allow for dynamically
 * changing buffers as capacity is exceeded.
 * </p>
 * <p>
 * As soon as a key or value is written, the cursor still maintain its
 * position but the initial view into the database is lost and will
 * need to be re-established.
 * </p>
 * <p>
 * Note that buffers write in native byte order by default which
 * may not be desired for certain key ordering schemes. The BufferCursor
 * writes data in regular big endian.
 * </p>
 * <p/>
 * BufferCopy is not thread safe and should be closed by the same thread
 * that opened it.
 * <p>
 * Do not use on Android.
 * </p>
 * <p/>
 * <pre>
 * {@code
 *
 * try (BufferCursor cursor = db.bufferCursor()) {
 *   cursor.first();
 *   while(cursor.next()) {
 *     cursor.keyByte(0);
 *     cursor.valByte(0);
 *   }
 *
 *   cursor.last();
 *   while(cursor.prev()) {
 *     cursor.keyByte(0);
 *     cursor.valByte(0);
 *   }
 *
 *   cursor.seek(bytes("London"));
 *   cursor.keyByte(0);
 *   cursor.valByte(0);
 * }
 * }
 * </pre>
 *
 * @author Kristoffer Sjögren
 */
public class BufferCursor implements AutoCloseable {
  private final Cursor cursor;
  private final Transaction tx;
  private DirectBuffer key;
  private DirectBuffer value;
  private ByteBuffer keyByteBuffer;
  private ByteBuffer valueByteBuffer;
  private boolean keyDatbaseMemoryLocation = false;
  private boolean valDatbaseMemoryLocation = false;
  private int keyWriteIndex = 0;
  private int valWriteIndex = 0;
  private final boolean isReadOnly;
  BufferCursor(Cursor cursor, Transaction tx, DirectBuffer key, DirectBuffer value) {
    this.cursor = cursor;
    this.isReadOnly = tx.isReadOnly();
    this.tx = tx;
    if (key.byteBuffer() == null) {
      throw new IllegalArgumentException("No ByteBuffer available for key.");
    }
    if (!key.byteBuffer().isDirect()) {
      throw new IllegalArgumentException("ByteBuffer for key must be direct.");
    }
    this.keyByteBuffer = key.byteBuffer();
    this.key = key;
    if (value.byteBuffer() == null) {
      throw new IllegalArgumentException("No ByteBuffer available for value.");
    }
    if (!value.byteBuffer().isDirect()) {
      throw new IllegalArgumentException("ByteBuffer for value must be direct.");
    }
    this.value = value;
    this.valueByteBuffer = value.byteBuffer();
  }

  BufferCursor(Cursor cursor, Transaction tx, int maxValueSize) {
    this(cursor, tx, new DirectBuffer(), new DirectBuffer(ByteBuffer.allocateDirect(maxValueSize)));
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
    int rc = cursor.seekPosition(this.key, value, SeekOp.RANGE);
    wrapDatabaseMemoryLocation(this.key, value);
    return rc == 0;
  }

  /**
   * Position at first key/data item.
   *
   * @return true if found
   */
  public boolean first() {
    int rc = cursor.position(key, value, GetOp.FIRST);
    wrapDatabaseMemoryLocation(key, value);
    return rc == 0;
  }

  /**
   * Position at last key/data item.
   *
   * @return true if found
   */
  public boolean last() {
    int rc = cursor.position(key, value, GetOp.LAST);
    wrapDatabaseMemoryLocation(key, value);
    return rc == 0;
  }

  /**
   * Position at next data item.
   *
   * @return true if found
   */
  public boolean next() {
    int rc = cursor.position(key, value, GetOp.NEXT);
    wrapDatabaseMemoryLocation(key, value);
    return rc == 0;
  }

  /**
   * Position at next data item of current key. Only for #MDB_DUPSORT.
   *
   * @return true if found
   */
  public boolean nextDup() {
    int rc = cursor.position(key, value, GetOp.NEXT_DUP);
    wrapDatabaseMemoryLocation(key, value);
    return rc == 0;
  }


  /**
   * Position at previous data item.
   *
   * @return true if found
   */
  public boolean prev() {
    int rc = cursor.position(key, value, GetOp.PREV);
    wrapDatabaseMemoryLocation(key, value);
    return rc == 0;
  }

  /**
   * Position at previous data item of current key. Only for #MDB_DUPSORT.
   *
   * @return true if found
   */
  public boolean prevDup() {
    int rc = cursor.position(key, value, GetOp.PREV_DUP);
    wrapDatabaseMemoryLocation(key, value);
    return rc == 0;
  }

  /**
   * <p>
   * Delete key/data pair at current cursor position.
   * </p>
   */
  public void delete() {
    cursor.delete();
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

  /**
   * Stores key/data pairs in the database replacing any
   * previously existing key.
   */
  public boolean put() {
    DirectBuffer k = (keyWriteIndex != 0) ?
      new DirectBuffer(key.addressOffset(), keyWriteIndex) : key;
    DirectBuffer v = (valWriteIndex != 0) ?
      new DirectBuffer(value.addressOffset(), valWriteIndex) : value;
    keyWriteIndex = 0;
    valWriteIndex = 0;
    try {
      return cursor.put(k, v, Constants.NOOVERWRITE) == 0;
    } catch (LMDBException e) {
      if (e.getErrorCode() == LMDBException.KEYEXIST) {
        return false;
      } else {
        throw e;
      }
    }
  }

  /**
   * Stores key/data pairs in the database replacing any
   * previously existing key.
   */
  public boolean overwrite() {
    DirectBuffer k = (keyWriteIndex != 0) ?
      new DirectBuffer(key.addressOffset(), keyWriteIndex) : key;
    DirectBuffer v = (valWriteIndex != 0) ?
      new DirectBuffer(value.addressOffset(), valWriteIndex) : value;
    keyWriteIndex = 0;
    valWriteIndex = 0;
    return cursor.put(k, v, 0) == 0;
  }

  /**
   * Append the given key/data pair to the end of the database.
   * No key comparisons are performed. This option allows
   * fast bulk loading when keys are already known to be in the
   * correct order. Loading unsorted keys with this flag will cause
   * data corruption.
   */
  public void append() {
    DirectBuffer k = (keyWriteIndex != 0) ?
      new DirectBuffer(key.addressOffset(), keyWriteIndex) : key;
    DirectBuffer v = (valWriteIndex != 0) ?
      new DirectBuffer(value.addressOffset(), valWriteIndex) : value;
    keyWriteIndex = 0;
    valWriteIndex = 0;
    cursor.put(k, v, Constants.APPEND);
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data byte
   * @return this
   */
  public BufferCursor keyWriteByte(int data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeKeyMemoryLocation();
    this.key.putByte(keyWriteIndex, (byte) data);
    keyWriteIndex += 1;
    return this;
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data int
   * @return this
   */
  public BufferCursor keyWriteInt(int data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeKeyMemoryLocation();
    this.key.putInt(keyWriteIndex, data, ByteOrder.BIG_ENDIAN);
    keyWriteIndex += 4;
    return this;
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data long
   * @return this
   */
  public BufferCursor keyWriteLong(long data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeKeyMemoryLocation();
    this.key.putLong(keyWriteIndex, data, ByteOrder.BIG_ENDIAN);
    keyWriteIndex += 8;
    return this;
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data float
   * @return this
   */
  public BufferCursor keyWriteFloat(float data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.key.putFloat(keyWriteIndex, data, ByteOrder.BIG_ENDIAN);
    keyWriteIndex += 4;
    return this;
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data double
   * @return this
   */
  public BufferCursor keyWriteDouble(double data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.key.putDouble(keyWriteIndex, data, ByteOrder.BIG_ENDIAN);
    keyWriteIndex += 8;
    return this;
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data string
   * @return this
   */
  public BufferCursor keyWriteUtf8(ByteString data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeKeyMemoryLocation();
    this.key.putString(keyWriteIndex, data);
    keyWriteIndex += data.size() + 1;
    return this;
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data string
   * @return this
   */
  public BufferCursor keyWriteUtf8(String data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeKeyMemoryLocation();
    ByteString bytes = new ByteString(data);
    this.key.putString(keyWriteIndex, bytes);
    keyWriteIndex += bytes.size() + 1;
    return this;
  }

  /**
   * Write data to key at current cursor position and
   * move write index forward.
   *
   * @param data byte array
   * @return this
   */
  public BufferCursor keyWriteBytes(byte[] data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeKeyMemoryLocation();
    this.key.putBytes(keyWriteIndex, data);
    keyWriteIndex += data.length;
    return this;
  }

  /**
   * Overwrite key data at current cursor position and
   * move write index forward.
   *
   * @param buffer   buffer
   * @param capacity capacity
   * @return this
   */
  public BufferCursor keyWrite(DirectBuffer buffer, int capacity) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    keyDatbaseMemoryLocation = false;
    this.key.wrap(buffer.addressOffset(), capacity);
    keyWriteIndex = capacity;
    return this;
  }

  /**
   * Get data from key at current cursor position.
   *
   * @param pos byte position
   * @return byte
   */
  public byte keyByte(int pos) {
    return this.key.getByte(pos);
  }

  /**
   * Get data from key at current cursor position.
   *
   * @param pos byte position
   * @return int
   */
  public int keyInt(int pos) {
    return this.key.getInt(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get data from key at current cursor position.
   *
   * @param pos byte position
   * @return long
   */
  public long keyLong(int pos) {
    return this.key.getLong(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get data from key at current cursor position.
   *
   * @param pos byte position
   * @return float
   */
  public float keyFloat(int pos) {
    return this.key.getFloat(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get data from key at current cursor position.
   *
   * @param pos byte position
   * @return double
   */
  public double keyDouble(int pos) {
    return this.key.getDouble(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get string data from key (ending with NULL byte)
   * at current cursor position.
   *
   * @param pos byte position
   * @return String
   */
  public ByteString keyUtf8(int pos) {
    return this.key.getString(pos);
  }

  /**
   * Get data from key at current cursor position.
   *
   * @param pos byte position
   * @return byte array
   */
  public byte[] keyBytes(int pos, int length) {
    byte[] k = new byte[length];
    key.getBytes(pos, k);
    return k;
  }

  /**
   * @return copy of key data
   */
  public byte[] keyBytes() {
    byte[] k = new byte[key.capacity()];
    key.getBytes(0, k);
    return k;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data byte
   * @return this
   */
  public BufferCursor valWriteByte(int data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.value.putByte(valWriteIndex, (byte) data);
    valWriteIndex += 1;
    return this;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data int
   * @return this
   */
  public BufferCursor valWriteInt(int data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.value.putInt(valWriteIndex, data, ByteOrder.BIG_ENDIAN);
    valWriteIndex += 4;
    return this;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data long
   * @return this
   */
  public BufferCursor valWriteLong(long data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.value.putLong(valWriteIndex, data, ByteOrder.BIG_ENDIAN);
    valWriteIndex += 8;
    return this;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data float
   * @return this
   */
  public BufferCursor valWriteFloat(float data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.value.putFloat(valWriteIndex, data, ByteOrder.BIG_ENDIAN);
    valWriteIndex += 4;
    return this;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data float
   * @return this
   */
  public BufferCursor valWriteDouble(double data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.value.putDouble(valWriteIndex, data, ByteOrder.BIG_ENDIAN);
    valWriteIndex += 8;
    return this;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data string
   * @return this
   */
  public BufferCursor valWriteUtf8(String data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    ByteString bytes = new ByteString(data);
    this.value.putString(valWriteIndex, bytes);
    valWriteIndex += bytes.size() + 1;
    return this;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data string
   * @return this
   */
  public BufferCursor valWriteUtf8(ByteString data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.value.putString(valWriteIndex, data);
    valWriteIndex += data.size() + 1;
    return this;
  }

  /**
   * Write data to value at current cursor position and
   * move write index forward.
   *
   * @param data byte array
   * @return this
   */
  public BufferCursor valWriteBytes(byte[] data) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    setSafeValMemoryLocation();
    this.value.putBytes(valWriteIndex, data);
    valWriteIndex += data.length;
    return this;
  }

  /**
   * Overwrite value data at current cursor position and
   * move write index forward.
   *
   * @param buffer   buffer
   * @param capacity capacity
   * @return this
   */
  public BufferCursor valWrite(DirectBuffer buffer, int capacity) {
    if (isReadOnly) {
      throw new LMDBException("Read only transaction", LMDBException.EACCES);
    }
    valDatbaseMemoryLocation = false;
    this.value.wrap(buffer.addressOffset(), capacity);
    valWriteIndex = capacity;
    return this;
  }

  /**
   * Get data from value at current cursor position.
   *
   * @param pos byte position
   * @return byte
   */
  public byte valByte(int pos) {
    return this.value.getByte(pos);
  }

  /**
   * Get data from value at current cursor position.
   *
   * @param pos byte position
   * @return int
   */
  public int valInt(int pos) {
    return this.value.getInt(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get data from value at current cursor position.
   *
   * @param pos byte position
   * @return long
   */
  public long valLong(int pos) {
    return this.value.getLong(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get data from key at current cursor position.
   *
   * @param pos byte position
   * @return byte array
   */
  public byte[] valBytes(int pos, int length) {
    byte[] v = new byte[length];
    value.getBytes(pos, v);
    return v;
  }


  /**
   * @return copy of value data
   */
  public byte[] valBytes() {
    byte[] v = new byte[value.capacity()];
    value.getBytes(0, v);
    return v;
  }

  /**
   * Get data from value at current cursor position.
   *
   * @param pos byte position
   * @return float
   */
  public float valFloat(int pos) {
    return this.value.getFloat(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get data from value at current cursor position.
   *
   * @param pos byte position
   * @return double
   */
  public double valDouble(int pos) {
    return this.value.getDouble(pos, ByteOrder.BIG_ENDIAN);
  }

  /**
   * Get string data from key (ending with NULL byte)
   * at current cursor position.
   *
   * @param pos byte position
   * @return String
   */
  public ByteString valUtf8(int pos) {
    return this.value.getString(pos);
  }

  private void setSafeKeyMemoryLocation() {
    if (keyDatbaseMemoryLocation) {
      this.key.wrap(keyByteBuffer);
      keyDatbaseMemoryLocation = false;
    }
  }

  private void setSafeValMemoryLocation() {
    if (valDatbaseMemoryLocation) {
      this.value.wrap(valueByteBuffer);
      valDatbaseMemoryLocation = false;
    }
  }

  private void wrapDatabaseMemoryLocation(DirectBuffer key, DirectBuffer value) {
    this.key = key;
    this.value = value;
    this.valDatbaseMemoryLocation = true;
    this.keyDatbaseMemoryLocation = true;
    keyWriteIndex = 0;
    valWriteIndex = 0;
  }
}
