package org.fusesource.lmdbjni;


import org.iq80.leveldb.DBIterator;
import org.openjdk.jmh.annotations.*;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.Map;

@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class Iteration extends Setup {

  static {
    initLMDB();
    initRocksDB();
    initLevelDb();
  }

  public static int rc = JNI.MDB_NOTFOUND;
  static Cursor cursor;

  @Benchmark
  public void lmdb_buffer_copy() {
    if (cursor == null) {
      Transaction tx = env.createTransaction();
      cursor = database.openCursor(tx);
    }
    if (rc == JNI.MDB_NOTFOUND) {
      Entry entry = cursor.get(GetOp.FIRST);
      // de-serialize key/value to make the test more realistic
      Bytes.getLong(entry.getKey(), 0);
      Bytes.getLong(entry.getValue(), 0);
    } else {
      Util.checkErrorCode(rc);
      Entry entry = cursor.get(GetOp.NEXT);
      // de-serialize key/value to make the test more realistic
      Bytes.getLong(entry.getKey(), 0);
      Bytes.getLong(entry.getValue(), 0);
    }
  }

  static DirectBuffer key = new DirectBuffer();
  static DirectBuffer value = new DirectBuffer();
  static BufferCursor bufferCursor;
  static boolean found = false;

  @Benchmark
  public void lmdb_zero_copy() {
    if (bufferCursor == null) {
      bufferCursor = database.bufferCursor(key, value);
    }
    if (!found) {
      found = bufferCursor.first();
      key.getLong(0);
      value.getLong(0);
    } else {
      found = bufferCursor.next();
      key.getLong(0);
      value.getLong(0);
    }
  }

  static RocksIterator it;

  @Benchmark
  public void rocksdb() throws IOException {
    if (it == null) {
      it = rocksDb.newIterator();
    }
    it.next();

    if (!it.isValid()) {
      it.seekToFirst();
    }
    it.key();
    it.value();
  }

  static DBIterator iterator;

  @Benchmark
  public void leveldb() throws IOException {
    if (iterator == null) {
      iterator = leveldb.iterator();
    }
    if (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> next = iterator.next();
    } else {
      iterator.seekToFirst();
    }
  }
}
