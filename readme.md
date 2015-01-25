# LMDB JNI

[![Build Status](https://travis-ci.org/deephacks/lmdbjni.png?branch=master)](https://travis-ci.org/deephacks/lmdbjni)
<a href="https://scan.coverity.com/projects/4017">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/4017/badge.svg"/>
</a>
[![Coverage Status](https://coveralls.io/repos/deephacks/lmdbjni/badge.png?branch=master)](https://coveralls.io/r/deephacks/lmdbjni?branch=master)

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

* [LMDB JNI Microbenchmark](http://pastebin.com/gPFVcakL), January 2015
   
  Iteration speed compared with <code>org.rocksdb.rocksdbjni</code> and <code>org.iq80.leveldb</code>.
   ```bash
   Benchmark                    Mode  Cnt         Score         Error  Units
   Iteration.leveldb           thrpt   10   7624941.049 ±  995999.362  ops/s
   Iteration.lmdb_buffer_copy  thrpt   10   3066605.928 ±  610793.399  ops/s
   Iteration.lmdb_zero_copy    thrpt   10  15029604.092 ± 1309367.614  ops/s
   Iteration.rocksdb           thrpt   10   1505814.770 ±  420279.355  ops/s
   ```
   

### Google groups

* https://groups.google.com/forum/#!forum/lmdbjni

### License

This project is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) but the binary jar it produces also includes `liblmdb` library version 0.9.14 of the OpenLDAP project which is licensed under the [The OpenLDAP Public License](http://www.openldap.org/software/release/license.html).

### Maven

```xml
<!-- required -->

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<!-- pick one or more platforms below -->

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

Recommended Package imports:
```java
 import org.fusesource.lmdbjni.*;
 import static org.fusesource.lmdbjni.Constants.*;
```

Opening and closing the database.
```java
 try (Env env = new Env("/tmp/mydb")) {
   try (Database db = env.openDatabase()) {
     ... // use the db
   }
 }
```

Putting, Getting, and Deleting key/values.
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

Performing transactional updates.

```java
 Transaction tx = env.createTransaction();
 boolean ok = false;
 try {
   db.delete(tx, bytes("Denver"));
   db.put(tx, bytes("Tampa"), bytes("green"));
   db.put(tx, bytes("London"), bytes("red"));
   ok = true;
 } finally {
   // Make sure you either commit or rollback to avoid resource leaks.
   if (ok) {
     tx.commit();
   } else {
     tx.abort();
   }
 }
```

Working against a Snapshot view of the Database.

```java
 // create a read-only transaction...
 Transaction tx = env.createTransaction(true);
 try {
   
   // All read operations will now use the same 
   // consistent view of the data.
   ... = db.db.openCursor(tx);
   ... = db.get(tx, bytes("Tampa"));

 } finally {
   // Make sure you commit the transaction to avoid resource leaks.
   tx.commit();
 }
```
Atomic hot backup.

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




The safest (and slowest) approach for interacting with LMDB JNI is using buffer copy using JNI as shown above. BufferCursor is an advanced, more efficient, zero copy mode. There is also DirectBuffer which is even more advanced but users should avoid interacting directly with these and use the BufferCursor API instead. Otherwise take extra care of buffer memory address+size and byte ordering. Mistakes may lead to SIGSEGV or unpredictable key ordering etc. This mode
is not available on Android.

```java
 try (BufferCursor cursor = db.bufferCursor()) {
   cursor.first();
   while(cursor.next()) {
     cursor.keyByte(0);
     cursor.valByte(0);
   }

   cursor.last();
   while(cursor.prev()) {
     cursor.keyBytes();
     cursor.valBytes();
   }

   cursor.seek(bytes("London"));
   cursor.keyUtf8(0);
   cursor.valUtf8(0);
 }
 
 // open for write
 try (BufferCursor cursor = db.bufferCursorWriter()) {
   cursor.first();
   cursor.keyWriteUtf8("England");
   cursor.valWriteUtf8("London");
   cursor.overwrite();
   cursor.first();
   curstor.delete();
 } 

```
