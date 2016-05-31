package org.fusesource.lmdbjni;

import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Buffer creation utility methods.
 */
final class Buffers {

  /**
   * Private constructor to prevent instantiation of utility class.
   */
  private Buffers() {
  }

  /**
   * Obtain a new buffer of the specified capacity.
   * <p>
   * Do not use on Android.
   *
   * @param capacity the number of bytes the buffer should store
   * @return a buffer (never null)
   */
  public static MutableDirectBuffer buffer(int capacity) {
    if (capacity == 0) {
      return new UnsafeBuffer(new byte[0]);
    }
    return new UnsafeBuffer(ByteBuffer.allocateDirect(capacity));
  }

  /**
   * Obtain a new buffer large enough for an LMDB maximum sized key. The current
   * maximum is provided by {@link Env#MAX_KEY_SIZE}.
   * <p>
   * Do not use on Android.
   *
   * @return a buffer suitable for storing a key (never null)
   */
  public static MutableDirectBuffer buffer() {
    return buffer(Env.MAX_KEY_SIZE);
  }

  /**
   * Obtain a new buffer slice that shares the same memory as the source buffer.
   * <p>
   * Do not use on Android.
   *
   * @param source the source buffer
   * @param length the number of bytes the new slice should include
   * @return a buffer of the request length, starting at byte 0 of the source
   *         (never null)
   */
  public static MutableDirectBuffer bufferSlice(DirectBuffer source, int length) {
    return new UnsafeBuffer(source.addressOffset(), length);
  }

}
