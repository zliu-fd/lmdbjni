package org.fusesource.lmdbjni;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TransactionTest {
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
    env = new Env(path);
    db = env.openDatabase();
  }

  @After
  public void after() throws IOException {
    db.close();
    env.close();
  }

  @Test
  public void testCommit() {
    Transaction tx = env.createTransaction(false);
    db.put(tx, data, data);
    tx.commit();
    assertArrayEquals(data, db.get(data));
  }

  @Test
  public void testAbort() {
    Transaction tx = env.createTransaction(false);
    db.put(tx, data, data);
    tx.abort();
    assertNull(db.get(data));
  }

  @Test
  public void testResetRenew() {
    testCommit();
    Transaction tx = env.createTransaction(true);
    assertArrayEquals(data, db.get(tx, data));
    tx.reset();
    try {
      db.get(tx, data);
      fail("should fail since transaction was committed");
    } catch (LMDBException e) {
      // ok
    }
    tx.renew();
    assertArrayEquals(data, db.get(tx, data));
    tx.commit();
  }

}
