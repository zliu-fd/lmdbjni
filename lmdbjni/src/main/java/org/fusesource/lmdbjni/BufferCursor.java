package org.fusesource.lmdbjni;

import java.io.Closeable;
import java.io.IOException;

public class BufferCursor implements Closeable {
  private final Cursor cursor;
  private final Transaction tx;
  private final DirectBuffer key;
  private final DirectBuffer value;

  BufferCursor(Cursor cursor, Transaction tx, DirectBuffer key, DirectBuffer value) {
    this.cursor = cursor;
    this.tx = tx;
    this.key = key;
    this.value = value;
  }

  public void first() {
    cursor.position(key, value, GetOp.FIRST);
  }

  public void last() {
    cursor.position(key, value, GetOp.LAST);
  }

  public void next() {
    cursor.position(key, value, GetOp.NEXT);
  }

  public void prev() {
    cursor.position(key, value, GetOp.PREV);
  }

  @Override
  public void close() throws IOException {
    if (tx != null) {
      tx.commit();
    }
    cursor.close();
  }
}
