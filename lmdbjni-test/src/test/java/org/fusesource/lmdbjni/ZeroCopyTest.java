package org.fusesource.lmdbjni;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Known issue, https://github.com/deephacks/lmdbjni/issues/13
 */
public class ZeroCopyTest {
    private File dir = new File("/tmp/test");
    private Database db;
    private Env env;

    private DirectBuffer k1 = new DirectBuffer(ByteBuffer.allocateDirect(8));
    private DirectBuffer v1 = new DirectBuffer(ByteBuffer.allocateDirect(8));
    private DirectBuffer k2 = new DirectBuffer(ByteBuffer.allocateDirect(8));
    private DirectBuffer v2 = new DirectBuffer(ByteBuffer.allocateDirect(8));

    @Before
    public void before() {
        if (db != null) {
            db.close();
        }
        if (env != null) {
            env.close();
        }

        Maven.recreateDir(dir);
        env = new Env();
        env.open(dir.getAbsolutePath());
        db = env.openDatabase("test");
    }

    @Test
    public void testPutAndGetAndDelete() throws Exception {
        k1.putLong(0, 10, ByteOrder.BIG_ENDIAN);
        v1.putLong(0, 11);
        k2.putLong(0, 12, ByteOrder.BIG_ENDIAN);
        v2.putLong(0, 13);

        db.put(k1, v1);
        db.put(k2, v2);

        DirectBuffer k = new DirectBuffer(ByteBuffer.allocateDirect(8));
        DirectBuffer v = new DirectBuffer(0, 0);
        k.putLong(0, 10, ByteOrder.BIG_ENDIAN);
        db.get(k, v);
        assertThat(v.getLong(0), is(11L));

        db.delete(k);
        try {
            db.get(k, v);
        } catch (LMDBException e) {
            assertThat(e.errorCode, is(LMDBException.NOTFOUND));
        }
    }

    @Test
    public void testCursorPutAndGet() throws Exception {
        k1.putLong(0, 14, ByteOrder.BIG_ENDIAN);
        v1.putLong(0, 15);
        k2.putLong(0, 16, ByteOrder.BIG_ENDIAN);
        v2.putLong(0, 17);

        Transaction tx = env.createTransaction();
        Cursor cursor = db.openCursor(tx);
        cursor.put(k1, v1, 0);
        cursor.put(k2, v2, 0);
        tx.commit();
        cursor.close();

        DirectBuffer k = new DirectBuffer(0, 0);
        DirectBuffer v = new DirectBuffer(0, 0);

        tx = env.createTransaction();
        cursor = db.openCursor(tx);

        cursor.position(k, v, GetOp.FIRST);
        assertThat(k.getLong(0, ByteOrder.BIG_ENDIAN), is(14L));
        assertThat(v.getLong(0), is(15L));

        cursor.position(k, v, GetOp.NEXT);
        assertThat(k.getLong(0, ByteOrder.BIG_ENDIAN), is(16L));
        assertThat(v.getLong(0), is(17L));
    }


    @Test
    public void testCursorSeekRange() throws Exception {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8);
        k1.putLong(0, 18, ByteOrder.BIG_ENDIAN);
        v1.putLong(0, 19);
        k2.putLong(0, 20, ByteOrder.BIG_ENDIAN);
        v2.putLong(0, 21);

        Transaction tx = env.createTransaction();
        Cursor cursor = db.openCursor(tx);
        cursor.put(k1, v1, 0);
        cursor.put(k2, v2, 0);
        tx.commit();
        cursor.close();

        tx = env.createTransaction();
        cursor = db.openCursor(tx);

        DirectBuffer k = new DirectBuffer(byteBuffer);
        DirectBuffer v = new DirectBuffer(0, 0);
        k.putLong(0, 10, ByteOrder.BIG_ENDIAN);

        cursor.seekPosition(k, v, SeekOp.RANGE);
        assertThat(k.getLong(0, ByteOrder.BIG_ENDIAN), is(18L));
        assertThat(v.getLong(0), is(19L));

        k.wrap(byteBuffer);
        k.putLong(0, 19, ByteOrder.BIG_ENDIAN);
        cursor.seekPosition(k, v, SeekOp.RANGE);
        assertThat(k.getLong(0, ByteOrder.BIG_ENDIAN), is(20L));
        assertThat(v.getLong(0), is(21L));
    }

}
