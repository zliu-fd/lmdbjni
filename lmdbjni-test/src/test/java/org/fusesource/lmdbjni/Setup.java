package org.fusesource.lmdbjni;

import java.io.File;

import static org.fusesource.lmdbjni.Constants.APPEND;

public class Setup {
    public static File dir = new File("/tmp/test");
    public static Database database;
    public static Env env = new Env();
    public static JNI.MDB_val valueVal;
    public static JNI.MDB_val keyVal;

    static {
        valueVal = new JNI.MDB_val();
        keyVal = new JNI.MDB_val();
        Maven.recreateDir(dir);
        env.open(dir.getAbsolutePath());
        env.setMapSize(4_294_967_296L);
        database = env.openDatabase("test");
        Transaction tx = env.createTransaction();
        for (int i = 0; i < 100000; i++) {
            long v = Long.reverseBytes(i);
            database.put(tx, Bytes.fromLong(i), Bytes.fromLong(v), APPEND);
        }
        tx.commit();
    }
}
