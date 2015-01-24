/**
 * Copyright (C) 2013, RedHat, Inc.
 *
 *    http://www.redhat.com/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fusesource.lmdbjni;

import java.io.Closeable;

import static org.fusesource.lmdbjni.JNI.*;
import static org.fusesource.lmdbjni.Util.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Env extends NativeObject implements Closeable {

  public static String version() {
    return string(JNI.MDB_VERSION_STRING);
  }
  private boolean open = false;

  public Env(String path) {
    super(create());
    setMaxDbs(1);
    open(path);
  }

  public Env() {
    super(create());
    setMaxDbs(1);
  }

  private static long create() {
    long env_ptr[] = new long[1];
    checkErrorCode(mdb_env_create(env_ptr));
    return env_ptr[0];
  }

  public void open(String path) {
    open(path, 0);
  }

  public void open(String path, int flags) {
    open(path, flags, 0644);
  }

  public void open(String path, int flags, int mode) {
    int rc = mdb_env_open(pointer(), path, flags, mode);
    if (rc != 0) {
      close();
    }
    checkErrorCode(rc);
    open = true;
  }

  /**
   * <p>
   * Close the environment and release the memory map.
   * </p>
   * Only a single thread may call this function. All transactions, databases,
   * and cursors must already be closed before calling this function. Attempts to
   * use any such handles after calling this function will cause a SIGSEGV.
   * The environment handle will be freed and must not be used again after this call.
   */
  @Override
  public void close() {
    if (self != 0) {
      mdb_env_close(self);
      self = 0;
    }
  }

  /**
   * <p>
   * Copy an LMDB environment to the specified path.
   * </p>
   * This function may be used to make a backup of an existing environment.
   * No lockfile is created, since it gets recreated at need.
   * This call can trigger significant file size growth if run in
   * parallel with write transactions, because it employs a read-only
   * transaction.
   *
   * @param path The directory in which the copy will reside. This
   *             directory must already exist and be writable but must otherwise be
   *             empty.
   */
  public void copy(String path) {
    checkArgNotNull(path, "path");
    checkErrorCode(mdb_env_copy(pointer(), path));
  }

  /**
   * <p>
   * Copy an LMDB environment to the specified path.
   * </p>
   * Perform compaction while copying: omit free pages and
   * sequentially renumber all pages in output. This option
   * consumes more CPU and runs more slowly than the default.
   *
   * @param path The directory in which the copy will reside. This
   *             directory must already exist and be writable but must otherwise be
   *             empty.
   */
  public void copyCompact(String path) {
    checkArgNotNull(path, "path");
    checkErrorCode(mdb_env_copy2(pointer(), path, 1));
  }

  /**
   * <p>
   * Flush the data buffers to disk.
   * </p>
   * Data is always written to disk when #mdb_txn_commit() is called,
   * but the operating system may keep it buffered. LMDB always flushes
   * the OS buffers upon commit as well, unless the environment was
   * opened with #MDB_NOSYNC or in part #MDB_NOMETASYNC.
   *
   * @param force force a synchronous flush.  Otherwise
   *              if the environment has the #MDB_NOSYNC flag set the flushes
   *              will be omitted, and with #MDB_MAPASYNC they will be asynchronous.
   */
  public void sync(boolean force) {
    checkErrorCode(mdb_env_sync(pointer(), force ? 1 : 0));
  }


  public void setMapSize(long size) {
    checkErrorCode(mdb_env_set_mapsize(pointer(), size));
  }

  public void setMaxDbs(long size) {
    checkErrorCode(mdb_env_set_maxdbs(pointer(), size));
  }

  public long getMaxReaders() {
    long rc[] = new long[1];
    checkErrorCode(mdb_env_get_maxreaders(pointer(), rc));
    return rc[0];
  }

  public void setMaxReaders(long size) {
    checkErrorCode(mdb_env_set_maxreaders(pointer(), size));
  }

  public int getFlags() {
    long[] flags = new long[1];
    checkErrorCode(mdb_env_get_flags(pointer(), flags));
    return (int) flags[0];
  }

  public void addFlags(int flags) {
    checkErrorCode(mdb_env_set_flags(pointer(), flags, 1));
  }

  public void removeFlags(int flags) {
    checkErrorCode(mdb_env_set_flags(pointer(), flags, 0));
  }

  public EnvInfo info() {
    MDB_envinfo rc = new MDB_envinfo();
    mdb_env_info(pointer(), rc);
    return new EnvInfo(rc);
  }

  public Stat stat() {
    MDB_stat rc = new MDB_stat();
    mdb_env_stat(pointer(), rc);
    return new Stat(rc);
  }

  /**
   * @see org.fusesource.lmdbjni.Env#createTransaction(Transaction, boolean)
   */
  public Transaction createTransaction() {
    return createTransaction(null, false);
  }

  /**
   * @see org.fusesource.lmdbjni.Env#createTransaction(Transaction, boolean)
   */
  public Transaction createTransaction(boolean readOnly) {
    return createTransaction(null, readOnly);
  }

  /**
   * @see org.fusesource.lmdbjni.Env#createTransaction(Transaction, boolean)
   */
  public Transaction createTransaction(Transaction parent) {
    return createTransaction(parent, false);
  }

  /**
   * <p>
   * Create a transaction for use with the environment.
   * </p>
   * <p/>
   * The transaction handle may be discarded using #mdb_txn_abort() or #mdb_txn_commit().
   *
   * @param parent   If this parameter is non-NULL, the new transaction
   *                 will be a nested transaction, with the transaction indicated by \b parent
   *                 as its parent. Transactions may be nested to any level. A parent
   *                 transaction and its cursors may not issue any other operations than
   *                 mdb_txn_commit and mdb_txn_abort while it has active child transactions.
   * @param readOnly This transaction will not perform any write operations.
   * @param parent   If this parameter is non-NULL, the new transaction
   *                 will be a nested transaction, with the transaction indicated by \b parent
   *                 as its parent. Transactions may be nested to any level. A parent
   *                 transaction and its cursors may not issue any other operations than
   *                 mdb_txn_commit and mdb_txn_abort while it has active child transactions.
   * @return transaction handle
   * @note A transaction and its cursors must only be used by a single
   * thread, and a thread may only have a single transaction at a time.
   * If #MDB_NOTLS is in use, this does not apply to read-only transactions.
   * @note Cursors may not span transactions.
   */
  public Transaction createTransaction(Transaction parent, boolean readOnly) {
    checkOpen();
    long txpointer[] = new long[1];
    checkErrorCode(mdb_txn_begin(pointer(), parent == null ? 0 : parent.pointer(), readOnly ? MDB_RDONLY : 0, txpointer));
    return new Transaction(txpointer[0]);
  }

  /**
   * <p>
   * Open a database in the environment.
   * </p>
   * <p/>
   * A database handle denotes the name and parameters of a database,
   * independently of whether such a database exists.
   * The database handle may be discarded by calling #mdb_dbi_close().
   * The old database handle is returned if the database was already open.
   * The handle may only be closed once.
   * The database handle will be private to the current transaction until
   * the transaction is successfully committed. If the transaction is
   * aborted the handle will be closed automatically.
   * After a successful commit the
   * handle will reside in the shared environment, and may be used
   * by other transactions. This function must not be called from
   * multiple concurrent transactions. A transaction that uses this function
   * must finish (either commit or abort) before any other transaction may
   * use this function.
   * <p/>
   * To use named databases (with name != NULL), #mdb_env_set_maxdbs()
   * must be called before opening the environment.  Database names
   * are kept as keys in the unnamed database.
   *
   * @param tx    A transaction handle.
   * @param name  The name of the database to open. If only a single
   *              database is needed in the environment, this value may be NULL.
   * @param flags Special options for this database. This parameter
   *              must be set to 0 or by bitwise OR'ing together one or more of the
   *              values described here.
   *              <ul>
   *              <li>#MDB_REVERSEKEY
   *              Keys are strings to be compared in reverse order, from the end
   *              of the strings to the beginning. By default, Keys are treated as strings and
   *              compared from beginning to end.
   *              <li>#MDB_DUPSORT
   *              Duplicate keys may be used in the database. (Or, from another perspective,
   *              keys may have multiple data items, stored in sorted order.) By default
   *              keys must be unique and may have only a single data item.
   *              <li>#MDB_INTEGERKEY
   *              Keys are binary integers in native byte order. Setting this option
   *              requires all keys to be the same size, typically sizeof(int)
   *              or sizeof(size_t).
   *              <li>#MDB_DUPFIXED
   *              This flag may only be used in combination with #MDB_DUPSORT. This option
   *              tells the library that the data items for this database are all the same
   *              size, which allows further optimizations in storage and retrieval. When
   *              all data items are the same size, the #MDB_GET_MULTIPLE and #MDB_NEXT_MULTIPLE
   *              cursor operations may be used to retrieve multiple items at once.
   *              <li>#MDB_INTEGERDUP
   *              This option specifies that duplicate data items are also integers, and
   *              should be sorted as such.
   *              <li>#MDB_REVERSEDUP
   *              This option specifies that duplicate data items should be compared as
   *              strings in reverse order.
   *              <li>#MDB_CREATE
   *              Create the named database if it doesn't exist. This option is not
   *              allowed in a read-only transaction or a read-only environment.
   * @return A database handle.
   */
  public Database openDatabase(Transaction tx, String name, int flags) {
    checkOpen();
    checkArgNotNull(tx, "tx");
    long dbi[] = new long[1];
    checkErrorCode(mdb_dbi_open(tx.pointer(), name, flags, dbi));
    return new Database(this, dbi[0]);
  }

  /**
   * @see org.fusesource.lmdbjni.Env#open(String, int, int)
   */
  public Database openDatabase() {
    return openDatabase(null, Constants.CREATE);
  }

  /**
   * @see org.fusesource.lmdbjni.Env#open(String, int, int)
   */
  public Database openDatabase(String name) {
    return openDatabase(name, Constants.CREATE);
  }

  /**
   * @see org.fusesource.lmdbjni.Env#open(String, int, int)
   */
  public Database openDatabase(String name, int flags) {
    Transaction tx = createTransaction();
    try {
      return openDatabase(tx, name, flags);
    } finally {
      tx.commit();
    }
  }

  public static void pushMemoryPool(int size) {
    NativeBuffer.pushMemoryPool(size);
  }

  public static void popMemoryPool() {
    NativeBuffer.popMemoryPool();
  }

  public long getMaxKeySize() {
    return mdb_env_get_maxkeysize(pointer());
  }

  private void checkOpen() {
    if (!open) {
      throw new LMDBException("Environment not open yet.");
    }
  }
}
