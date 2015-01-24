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

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class LMDBException extends RuntimeException {

  private static final long serialVersionUID = 8823282287521553903L;
  /** key/data pair already exists */
  public static final int KEYEXIST = JNI.MDB_KEYEXIST;

  /** key/data pair not found (EOF) */
  public static final int NOTFOUND = JNI.MDB_NOTFOUND;

  /** Requested page not found - this usually indicates corruption */
  public static final int PAGE_NOTFOUND = JNI.MDB_PAGE_NOTFOUND;

  /** Located page was wrong type */
  public static final int CORRUPTED = JNI.MDB_CORRUPTED;

  /** Update of meta page failed, probably I/O error */
  public static final int PANIC = JNI.MDB_PANIC;

  /** Environment version mismatch */
  public static final int VERSION_MISMATCH = JNI.MDB_VERSION_MISMATCH;

  /** File is not a valid LMDB file */
  public static final int INVALID = JNI.MDB_INVALID;

  /** Environment mapsize reached */
  public static final int MAP_FULL = JNI.MDB_MAP_FULL;

  /** Environment maxdbs reached */
  public static final int DBS_FULL = JNI.MDB_DBS_FULL;

  /** Environment maxreaders reached */
  public static final int READERS_FULL = JNI.MDB_READERS_FULL;

  /** Too many TLS keys in use - Windows only */
  public static final int TLS_FULL = JNI.MDB_TLS_FULL;

  /** Txn has too many dirty pages */
  public static final int TXN_FULL = JNI.MDB_TXN_FULL;

  /** Cursor stack too deep - internal error */
  public static final int CURSOR_FULL = JNI.MDB_CURSOR_FULL;

  /** Page has not enough space - internal error */
  public static final int PAGE_FULL = JNI.MDB_PAGE_FULL;

  /** Database contents grew beyond environment mapsize */
  public static final int MAP_RESIZED = JNI.MDB_MAP_RESIZED;

  /** MDB_INCOMPATIBLE: Operation and DB incompatible, or DB flags changed */
  public static final int INCOMPATIBLE = JNI.MDB_INCOMPATIBLE;

  /** Invalid reuse of reader locktable slot */
  public static final int BAD_RSLOT = JNI.MDB_BAD_RSLOT;

  int errorCode;

  public LMDBException() {
  }

  public LMDBException(String message) {
    super(message);
  }

  public LMDBException(String message, int errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }
}
