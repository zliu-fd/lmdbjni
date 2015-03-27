package org.fusesource.lmdbjni;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

import static org.fusesource.lmdbjni.Bytes.fromLong;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

public class DatabaseTest {
  static {
    Setup.setLmdbLibraryPath();
  }

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  Env env;
  Database db;
  byte[] data = new byte[] {1,2,3};

  @Before
  public void before() throws IOException {
    String path = tmp.newFolder().getCanonicalPath();
    env = new Env();
    env.setMapSize(16 * 4096);
    env.open(path);
    db = env.openDatabase();
  }

  @After
  public void after() {
    db.close();
    env.close();
  }

  /**
   * Should trigger MDB_MAP_FULL if entries wasn't deleted.
   */
  @Test
  public void testCleanupFullDb() {
    for (int i = 0; i < 100; i++) {
      // twice the size of page size
      db.put(fromLong(i), new byte[2 * 4096]);
      db.delete(fromLong(i));
    }
  }

  @Test
  public void testDrop() {
    byte[] bytes = {1,2,3};
    db.put(bytes, bytes);
    byte[] value = db.get(bytes);
    assertArrayEquals(value, bytes);
    // empty
    db.drop(false);
    value = db.get(bytes);
    assertNull(value);
    db.put(bytes, bytes);
    db.drop(true);
    try {
      db.get(bytes);
      fail("db has been closed");
    } catch (LMDBException e) {

    }
  }

  @Test
  public void testStat() {
    db.put(new byte[]{1}, new byte[]{1});
    db.put(new byte[]{2}, new byte[]{1});
    Stat stat = db.stat();
    System.out.println(stat);
    assertThat(stat.getEntries(), is(2L));
    assertThat(stat.getPsize(), is(not(0L)));
    assertThat(stat.getOverflowPages(), is(0L));
    assertThat(stat.getDepth(), is(1L));
    assertThat(stat.getLeafPages(), is(1L));
  }

  @Test
  public void testDeleteBuffer() {
    db.put(new byte[]{1}, new byte[]{1});
    DirectBuffer key = new DirectBuffer(ByteBuffer.allocateDirect(1));
    key.putByte(0, (byte) 1);
    db.delete(key);
    assertNull(db.get(new byte[]{1}));
  }

  @Test
  public void testSetComparatorAsc() {
    Transaction writeTransaction = env.createWriteTransaction();
    db.setComparator(writeTransaction, new Comparator<byte[]>() {
      @Override
      public int compare(byte[] key1, byte[] key2) {
        return (int) (Bytes.getLong(key1) - Bytes.getLong(key2));
      }
    });
    writeTransaction.commit();
    writeTransaction = env.createWriteTransaction();

    for (int i = 0; i < 1000; i++) {
      db.put(writeTransaction, Bytes.fromLong(i), Bytes.fromLong(i));
    }
    writeTransaction.commit();
    try (EntryIterator it = db.iterate(env.createReadTransaction()) ){
      long prev = -1;
      while(it.hasNext()) {
        if (prev == -1) {
          prev = Bytes.getLong(it.next().getKey());
        } else {
          long now = Bytes.getLong(it.next().getKey());
          assertThat((prev+1), is(now));
          prev = now;
        }
      }
    }
  }

  @Test
  public void testSetComparatorDesc() {
    Transaction writeTransaction = env.createWriteTransaction();
    db.setComparator(writeTransaction, new Comparator<byte[]>() {
      @Override
      public int compare(byte[] key1, byte[] key2) {
        return (int) (Bytes.getLong(key2) - Bytes.getLong(key1));
      }
    });
    writeTransaction.commit();
    writeTransaction = env.createWriteTransaction();

    for (int i = 0; i < 1000; i++) {
      db.put(writeTransaction, Bytes.fromLong(i), Bytes.fromLong(i));
    }
    writeTransaction.commit();
    try (EntryIterator it = db.iterate(env.createReadTransaction()) ){
      long prev = -1;
      while(it.hasNext()) {
        if (prev == -1) {
          prev = Bytes.getLong(it.next().getKey());
        } else {
          long now = Bytes.getLong(it.next().getKey());
          assertThat((prev-1), is(now));
          prev = now;
        }
      }
    }
  }


  @Test
  public void testSetDirectComparatorAsc() {
    Transaction writeTransaction = env.createWriteTransaction();
    db.setDirectComparator(writeTransaction, new Comparator<DirectBuffer>() {
      @Override
      public int compare(DirectBuffer key1, DirectBuffer key2) {
        return (int) (key1.getLong(0) - key2.getLong(0));
      }
    });
    writeTransaction.commit();
    writeTransaction = env.createWriteTransaction();

    for (int i = 0; i < 1000; i++) {
      db.put(writeTransaction, Bytes.fromLong(i), Bytes.fromLong(i));
    }
    writeTransaction.commit();
    try (EntryIterator it = db.iterate(env.createReadTransaction()) ){
      long prev = -1;
      while(it.hasNext()) {
        if (prev == -1) {
          prev = Bytes.getLong(it.next().getKey());
        } else {
          long now = Bytes.getLong(it.next().getKey());
          assertThat((prev+1), is(now));
          prev = now;
        }
      }
    }
  }

  @Test
  public void testSetDirectComparatorDesc() {
    Transaction writeTransaction = env.createWriteTransaction();
    db.setDirectComparator(writeTransaction, new Comparator<DirectBuffer>() {
      @Override
      public int compare(DirectBuffer key1, DirectBuffer key2) {
        return (int) (key2.getLong(0) - key1.getLong(0));
      }
    });
    writeTransaction.commit();
    writeTransaction = env.createWriteTransaction();

    for (int i = 0; i < 1000; i++) {
      db.put(writeTransaction, Bytes.fromLong(i), Bytes.fromLong(i));
    }
    writeTransaction.commit();
    try (EntryIterator it = db.iterate(env.createReadTransaction()) ){
      long prev = -1;
      while(it.hasNext()) {
        if (prev == -1) {
          prev = Bytes.getLong(it.next().getKey());
        } else {
          long now = Bytes.getLong(it.next().getKey());
          assertThat((prev-1), is(now));
          prev = now;
        }
      }
    }
  }
}
