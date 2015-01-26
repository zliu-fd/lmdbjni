package org.fusesource.lmdbjni;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for entries.
 *
 * <pre>
 * {@code
 * try (EntryIterator it = db.iterate()) {
 *   for (Entry next : it.iterable()) {
 *   }
 * }
 * }
 * </pre>
 */
public class EntryIterator implements Iterator<Entry>, AutoCloseable {
  private final Cursor cursor;
  private final IteratorType type;
  private final Transaction tx;
  private final byte[] key;

  EntryIterator(Cursor cursor, Transaction tx, byte[] key, IteratorType type) {
    this.cursor = cursor;
    this.type = type;
    this.tx = tx;
    this.key = key;
    if (key != null) {
      this.entry = cursor.seek(SeekOp.KEY, key);
    } else {
      this.entry = cursor.get(GetOp.FIRST);
    }
  }

  private Entry entry;
  private boolean first = true;

  @Override
  public boolean hasNext() {
    if (first) {
      if (key != null) {
        this.entry = cursor.seek(SeekOp.KEY, key);
      } else {
        if (type == IteratorType.FORWARD) {
          this.entry = cursor.get(GetOp.FIRST);
        } else {
          this.entry = cursor.get(GetOp.LAST);
        }
      }
      first = false;
      if (entry == null) {
        return false;
      }
    } else {
      if (type == IteratorType.FORWARD) {
        this.entry = cursor.get(GetOp.NEXT);
      } else {
        this.entry = cursor.get(GetOp.PREV);
      }
      if (entry == null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Entry next() throws NoSuchElementException {
    if (entry == null) {
      throw new NoSuchElementException();
    }
    return entry;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Close the cursor and the transaction.
   */
  @Override
  public void close() {
    if (tx != null) {
      tx.commit();
    }
    cursor.close();
  }

  public Iterable<Entry> iterable() {
    return new Iterable<Entry>() {
      @Override
      public Iterator<Entry> iterator() {
        return EntryIterator.this;
      }
    };
  }

  static enum IteratorType {
    FORWARD, BACKWARD
  }
}
