package org.fusesource.lmdbjni;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.fusesource.lmdbjni.Constants.APPEND;

public class Setup {

  public static File dir = new File("/tmp/test");
  public static Database database;
  public static Env env;
  public static JNI.MDB_val valueVal;
  public static JNI.MDB_val keyVal;

  public static void initLMDB() {
    setLmdbLibraryPath();
    valueVal = new JNI.MDB_val();
    keyVal = new JNI.MDB_val();
    Maven.recreateDir(dir);
    env = new Env();
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

  static RocksDB rocksDb;

  public static void initRocksDB() {
    Maven.recreateDir(dir);
    org.rocksdb.Options options = new org.rocksdb.Options();
    options.setCreateIfMissing(true);
    try {
      rocksDb = RocksDB.open(options, dir.getAbsolutePath());
      for (int i = 0; i < 100000; i++) {
        rocksDb.put(Bytes.fromLong(i), Bytes.fromLong(i));
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  static DB leveldb;

  public static void initLevelDb() {
    Maven.recreateDir(dir);
    try {
      leveldb = Iq80DBFactory.factory.open(dir, new org.iq80.leveldb.Options());
      for (int i = 0; i < 100000; i++) {
        leveldb.put(Bytes.fromLong(i), Bytes.fromLong(i));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static String OS = System.getProperty("os.name").toLowerCase();

  public static boolean isWindows() {
    return (OS.indexOf("win") >= 0);
  }

  public static boolean isMac() {
    return (OS.indexOf("mac") >= 0);
  }

  public static void setLmdbLibraryPath() {
    File path = computeMavenProjectRoot(Setup.class);
    if (isWindows()) {
      // TODO
    } else if (isMac()) {
      path = new File(path, "../lmdbjni-osx64/target/native-build/target/lib");
    } else {
      path = new File(path, "../lmdbjni-linux64/target/native-build/target/lib");
    }
    if (!path.exists()) {
      throw new IllegalStateException("Please build lmdbjni first " +
        "with a platform specific profile");
    }
    System.setProperty("library.lmdbjni.path", path.getAbsolutePath());
  }

  private static File computeMavenProjectRoot(Class<?> anyTestClass) {
    final String clsUri = anyTestClass.getName().replace('.', '/') + ".class";
    final URL url = anyTestClass.getClassLoader().getResource(clsUri);
    final String clsPath = url.getPath();
    // located in ./target/junit-classes or ./eclipse-out/target
    final File target_test_classes = new File(clsPath.substring(0,
      clsPath.length() - clsUri.length()));
    // lookup parent's parent
    return target_test_classes.getParentFile().getParentFile();
  }
}
