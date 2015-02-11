package org.fusesource.lmdbjni;


import org.iq80.leveldb.DBIterator;
import org.openjdk.jmh.annotations.*;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fusesource.lmdbjni.Setup.initMapDB;

@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class Iteration extends Setup {
  static boolean found = false;

  static {
    initLMDB();
    initRocksDB();
    initLevelDb();
    initMapDB();
  }

  public static int rc = JNI.MDB_NOTFOUND;
  static Cursor cursor;

//  @Benchmark
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

  static BufferCursor bufferCursor;
  static Transaction tx;

//  @Benchmark
  public void lmdb_zero_copy() {
    if (bufferCursor == null) {
      tx = env.createReadTransaction();
      bufferCursor = database.bufferCursor(tx);
    }
    if (!found) {
      found = bufferCursor.first();
      bufferCursor.keyLong(0);
      bufferCursor.valLong(0);
    } else {
      found = bufferCursor.next();
      bufferCursor.keyLong(0);
      bufferCursor.valLong(0);
    }
  }
  static AtomicInteger counter = new AtomicInteger(0);
  static byte[] bytes = null;

  @Benchmark
  public void mapdb() {
    if (bytes == null) {
      counter.set(0);
    } 
    bytes = Setup.mapdbmap.get(Bytes.fromLong(counter.incrementAndGet()));
  }

  static RocksIterator it;

//  @Benchmark
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

//  @Benchmark
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
