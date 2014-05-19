# LMDB JNI

## Description

LMDB JNI gives you a Java interface to the 
[OpenLDAP Lightning Memory-Mapped Database](http://symas.com/mdb/) library
which is a fast key-value storage library written for OpenLDAP project.

This is a fork that build on the work found at https://github.com/chirino/lmdbjni.

## Using Prebuilt Jar

The prebuilt binary jars only work on 64 bit OS X or Linux machines.

### License

This project is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) but the binary jar it produces also includes `liblmdb` library version 0.9.10 of the OpenLDAP project which is licensed under the [The OpenLDAP Public License](http://www.openldap.org/software/release/license.html).

### Using as a Maven Dependency

Add one (or both) of the following dependency to your Maven `pom.xml`.

```xml

<dependency>
    <groupId>org.deephacks.lmdbjni</groupId>
    <artifactId>lmdbjni-linux64</artifactId>
    <version>0.1.1</version>
</dependency>

<dependency>
    <groupId>org.deephacks.lmdbjni</groupId>
    <artifactId>lmdbjni-osx64</artifactId>
    <version>0.1.1</version>
</dependency>
```

## API Usage:

Recommended Package imports:

    import org.fusesource.lmdbjni.*;
    import static org.fusesource.lmdbjni.Constants.*;

Opening and closing the database.

    Env env = new Env();
    try {
      env.open("/tmp/mydb");
      Database db = env.openDatabase("foo");
      
      ... // use the db
      db.close();
    } finally {
      // Make sure you close the env to avoid resource leaks.
      env.close();
    }

Putting, Getting, and Deleting key/values.

    db.put(bytes("Tampa"), bytes("rocks"));
    String value = string(db.get(bytes("Tampa")));
    db.delete(bytes("Tampa"));

Performing Atomic/Transacted Updates:

    Transaction tx = env.createTransaction();
    boolean ok = false;
    try {
      db.delete(tx, bytes("Denver"));
      db.put(tx, bytes("Tampa"), bytes("green"));
      db.put(tx, bytes("London"), bytes("red"));
      ok = true;
    } finally {
      // Make sure you either commit or rollback to avoid resource leaks.
      if( ok ) {
        tx.commit();
      } else {
        tx.abort();
      }
    }

Working against a Snapshot view of the Database:

    // cerate a read-only transaction...
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

Iterating key/values:

    Transaction tx = env.createTransaction(true);
    try {
      Cursor cursor = db.openCursor(tx);
      try {
        for( Entry entry = cursor.get(FIRST); entry !=null; entry = cursor.get(NEXT) ) {
            String key = string(entry.getKey());
            String value = string(entry.getValue());
            System.out.println(key+" = "+value);
        }
      } finally {
        // Make sure you close the cursor to avoid leaking reasources.
        cursor.close();
      }

    } finally {
      // Make sure you commit the transaction to avoid resource leaks.
      tx.commit();
    }

Using a memory pool to make native memory allocations more efficient:

    Env.pushMemoryPool(1024 * 512);
    try {
        // .. work with the DB in here, 
    } finally {
        Env.popMemoryPool();
    }

