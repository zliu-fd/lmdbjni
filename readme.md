# LMDB JNI

[![Build Status](https://travis-ci.org/deephacks/lmdbjni.png?branch=master)](https://travis-ci.org/deephacks/lmdbjni) [![Coverage Status](https://coveralls.io/repos/deephacks/lmdbjni/badge.svg?branch=master)](https://coveralls.io/r/deephacks/lmdbjni?branch=master) <a href="https://scan.coverity.com/projects/4017">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/4017/badge.svg"/>
</a>

LMDB JNI provide a Java API to [LMDB](http://symas.com/mdb/) which is an ultra-fast, ultra-compact key-value embedded data store developed by Symas for the OpenLDAP Project. It uses memory-mapped files, so it has the read performance of a pure in-memory database while still offering the persistence of standard disk-based databases. Transactional with full ACID semantics and crash-proof by design. No corruption. No startup time. No dependencies.

LMDB JNI is available for 64 bit Linux, OSX, Windows and Android.

### References

 * [LMDB C API reference](http://symas.com/mdb/doc/group__internal.html)
 * [LMDB paper](http://symas.com/mdb/20120829-LinuxCon-MDB-txt.pdf)
 * [LMDB lecture by Howard Chu](https://www.parleys.com/play/517f58f9e4b0c6dcd95464ae/)
 * [LMDB source code](https://gitorious.org/mdb/mdb/source/libraries/liblmdb)
 * [LMDB JNI JavaDoc](http://deephacks.org/lmdbjni/apidocs/index.html)

### Benchmarks

* [In-Memory Microbenchmark] (http://symas.com/mdb/inmem), June 2014

   Multithreaded read performance for a purely in-memory database.

* [On-Disk Microbenchmark](http://symas.com/mdb/ondisk), November 2014

   Multithreaded read performance for a database that is over 5 times larger than the size of RAM.

* [LMDB JNI Microbenchmark](http://pastebin.com/3huizUps), February 2015 ([source](https://github.com/deephacks/lmdbjni/blob/master/jmh/src/main/java/org/fusesource/lmdbjni/Iteration.java))
   
  Row scanning speed per second compared with [RocksDB](https://github.com/facebook/rocksdb), [LevelDB](https://github.com/dain/leveldb) and [MapDB](https://github.com/jankotek/MapDB).
   Mongodb is difficult to setup in JMH but [de.flapdoodle.embed.mongo](https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo) indicate that it is around 50x slower than lmdb_zero_copy.
   ```bash
  Benchmark                    Mode  Cnt         Score         Error  Units
  Iteration.leveldb           thrpt   10   6965637.351 ±  784589.894  ops/s
  Iteration.lmdb_buffer_copy  thrpt   10   3157796.643 ±  265830.424  ops/s
  Iteration.lmdb_zero_copy    thrpt   10  16372428.882 ± 1812316.504  ops/s
  Iteration.mapdb             thrpt   10   1358748.670 ±   87502.413  ops/s
  Iteration.rocksdb           thrpt   10   1311441.804 ±  176129.883  ops/s
   ```

### Maven

```xml
<!-- required java classes -->

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<!-- prebuilt liblmdb platform packages -->

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-linux64</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-osx64</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-win64</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-android</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

```

### Usage

Recommended package imports.
```java
 import org.fusesource.lmdbjni.*;
 import static org.fusesource.lmdbjni.Constants.*;
```

[Opening](http://deephacks.org/lmdbjni/apidocs/org/fusesource/lmdbjni/Env.html#open-java.lang.String-int-int-) and closing the database.
```java
 try (Env env = new Env("/tmp/mydb")) {
   try (Database db = env.openDatabase()) {
     ... // use the db
   }
 }
```

[Putting](http://deephacks.org/lmdbjni/apidocs/org/fusesource/lmdbjni/Database.html#put-org.fusesource.lmdbjni.Transaction-byte:A-byte:A-int-), [getting](http://deephacks.org/lmdbjni/apidocs/org/fusesource/lmdbjni/Database.html#get-org.fusesource.lmdbjni.Transaction-byte:A-), and [deleting](http://deephacks.org/lmdbjni/apidocs/org/fusesource/lmdbjni/Database.html#delete-org.fusesource.lmdbjni.Transaction-byte:A-byte:A-) key/values.
```java
 db.put(bytes("Tampa"), bytes("rocks"));
 String value = string(db.get(bytes("Tampa")));
 db.delete(bytes("Tampa"));
```

Iterating and seeking key/values forward and backward.

```java
try (EntryIterator it = db.iterate()) {
  for (Entry next : it.iterable()) {
  }
}

try (EntryIterator it = db.iterateBackward()) {
  for (Entry next : it.iterable()) {
  }
}

byte[] key = bytes("London");
try (EntryIterator it = db.seek(key)) {
  for (Entry next : it.iterable()) {
  }
}

try (EntryIterator it = db.seekBackward(key))) {
  for (Entry next : it.iterable()) {
  }
}

```

Performing [transactional](http://deephacks.org/lmdbjni/apidocs/org/fusesource/lmdbjni/Transaction.html) updates.

```java
 try (Transaction tx = env.createWriteTransaction()) {
   db.delete(tx, bytes("Denver"));
   db.put(tx, bytes("Tampa"), bytes("green"));
   db.put(tx, bytes("London"), bytes("red"));
   tx.commit();  // if commit is not called, the transaction is aborted
 }
```

Working against a snapshot view of the database.

```java
 // create a read-only transaction...
 try (Transaction tx = env.createReadTransaction()) {
   
   // All read operations will now use the same 
   // consistent view of the data.
   ... = db.db.openCursor(tx);
   ... = db.get(tx, bytes("Tampa"));
 }
```
Atomic hot [backup](http://deephacks.org/lmdbjni/apidocs/org/fusesource/lmdbjni/Env.html#copy-java.lang.String-).

```java
 env.copy(backupPath);
```

Using a memory pool to make native memory allocations more efficient:

```java
 Env.pushMemoryPool(1024 * 512);
 try {
     // .. work with the DB in here, 
 } finally {
     Env.popMemoryPool();
 }
```

### Zero copy usage

The safest (and least efficient) approach for interacting with LMDB JNI is using buffer copy using JNI as shown above. [BufferCursor](http://deephacks.org/lmdbjni/apidocs/org/fusesource/lmdbjni/BufferCursor.html) is an advanced, more efficient, zero copy mode. This mode is not available on Android.

*There is also DirectBuffer which is even more advanced but users should avoid interacting directly with these and use the BufferCursor API instead. Otherwise take extra care of buffer memory address+size and byte ordering. Mistakes may lead to SIGSEGV or unpredictable key ordering etc.*

```java
 // read only
 try (BufferCursor cursor = db.bufferCursor()) {

   // iterate from first item and forwards
   cursor.first();
   while(cursor.next()) {
     // read a position in buffer
     cursor.keyByte(0);
     cursor.valByte(0);
   }

   // iterate from last item and backwards
   cursor.last();
   while(cursor.prev()) {
     // copy entire buffer
     cursor.keyBytes();
     cursor.valBytes();
   }

   // find first key greater than or equal to specified key.
   cursor.seek(bytes("London"));
   // read utf-8 string from position until NULL byte
   cursor.keyUtf8(0);
   cursor.valUtf8(0);
 }
 
 // open for write
 try (BufferCursor cursor = db.bufferCursorWriter()) {
   cursor.first();
   // write utf-8 ending with NULL byte
   cursor.keyWriteUtf8("England");
   cursor.valWriteUtf8("London");
   // overwrite existing item if any. Data is not written
   // into database before this operation is called and
   // no updates are visible outside this transaction until
   // the transaction is committed
   cursor.overwrite();
   cursor.first();
   // delete current cursor position
   curstor.delete();
 } 

```

### Google groups

* https://groups.google.com/forum/#!forum/lmdbjni

### License

This project is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) but the binary jar it produces also includes `liblmdb` library version 0.9.14 of the OpenLDAP project which is licensed under the [The OpenLDAP Public License](http://www.openldap.org/software/release/license.html).
