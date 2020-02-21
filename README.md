SQL Dim
=========

A lightweight wrapper around `SupportSQLiteOpenHelper` and `ContentResolver` which introduces reactive
stream semantics to queries.

Usage
-----

Create a `SqlDim` instance which is an adapter for the library functionality.

```java
SqlDim<Object> sqlDim = new SqlDim.Builder<>().build();
```

Pass a `SupportSQLiteOpenHelper` instance and a `Scheduler` to create a `DimDatabase`.

```java
DimDatabase<Object> db = sqlDim.wrapDatabaseHelper(openHelper, Schedulers.io());
```

A `Scheduler` is required for a few reasons, but the most important is that query notifications can
trigger on the thread of your choice. The query can then be run without blocking the main thread or
the thread which caused the trigger.

The `DimDatabase.createQuery` method is similar to `SupportSQLiteDatabase.query` except it takes an
additional parameter of table(s) on which to listen for changes. Subscribe to the returned
`Observable<Query>` which will immediately notify with a `Query` to run.

```java
Observable<Query> users = db.createQuery("users", "SELECT * FROM users");
users.subscribe(new Consumer<Query>() {
  @Override public void accept(Query query) {
    Cursor cursor = query.run();
    // TODO parse data...
  }
});
```

Unlike a traditional `query`, updates to the specified table(s) will trigger additional
notifications for as long as you remain subscribed to the observable. This means that when you
insert, update, or delete data, any subscribed queries will update with the new data instantly.

```java
final AtomicInteger queries = new AtomicInteger();
users.subscribe(new Consumer<Query>() {
  @Override public void accept(Query query) {
    queries.getAndIncrement();
  }
});
System.out.println("Queries: " + queries.get()); // Prints 1

db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("jw", "Jake Wharton"));
db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("mattp", "Matt Precious"));
db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("strong", "Alec Strong"));

System.out.println("Queries: " + queries.get()); // Prints 4
```

In the previous example we re-used the `DimDatabase` object "db" for inserts. All insert, update,
or delete operations must go through this object in order to correctly notify subscribers.

Unsubscribe from the returned `Subscription` to stop getting updates.

```java
final AtomicInteger queries = new AtomicInteger();
Subscription s = users.subscribe(new Consumer<Query>() {
  @Override public void accept(Query query) {
    queries.getAndIncrement();
  }
});
System.out.println("Queries: " + queries.get()); // Prints 1

db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("jw", "Jake Wharton"));
db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("mattp", "Matt Precious"));
s.unsubscribe();

db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("strong", "Alec Strong"));

System.out.println("Queries: " + queries.get()); // Prints 3
```

Use transactions to prevent large changes to the data from spamming your subscribers.

```java
final AtomicInteger queries = new AtomicInteger();
users.subscribe(new Consumer<Query>() {
  @Override public void accept(Query query) {
    queries.getAndIncrement();
  }
});
System.out.println("Queries: " + queries.get()); // Prints 1

Transaction<Object> transaction = db.newTransaction();
try {
  db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("jw", "Jake Wharton"));
  db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("mattp", "Matt Precious"));
  db.insert("users", SQLiteDatabase.CONFLICT_ABORT, createUser("strong", "Alec Strong"));
  transaction.markSuccessful();
} finally {
  transaction.end();
}

System.out.println("Queries: " + queries.get()); // Prints 2
```
*Note: You can also use try-with-resources with a `Transaction` instance.*

Since queries are just regular RxJava `Observable` objects, operators can also be used to
control the frequency of notifications to subscribers.

```java
users.debounce(500, MILLISECONDS).subscribe(new Consumer<Query>() {
  @Override public void accept(Query query) {
    // TODO...
  }
});
```

Additionally, you can use a `MarkedQuery` to observe specific change operations by marking them:

```java
Observable<MarkedQuery<Object>> markedUsers = db.createMarkedQuery("users", "SELECT * FROM users");

final Set<Object> markers = Collections.synchronizedSet(new HashSet<>());
markedUsers.subscribe(new Consumer<MarkedQuery<Object>>() {
  @Override public void accept(MarkedQuery<Object> markedQuery) {
    markers.addAll(markedQuery.markers);
  }
});
System.out.println("Markers: " + markers); // Prints empty

Transaction<Object> transaction = db.newTransaction();
try {
  db.insertMarked("marker1", "users", SQLiteDatabase.CONFLICT_ABORT, createUser("jw", "Jake Wharton"));
  db.insertMarked("marker2", "users", SQLiteDatabase.CONFLICT_ABORT, createUser("mattp", "Matt Precious"));
  db.insertMarked("marker3", "users", SQLiteDatabase.CONFLICT_ABORT, createUser("strong", "Alec Strong"));
  transaction.markSuccessful("marker4");
} finally {
  transaction.end();
}

System.out.println("Markers: " + markers); // Prints some ordering of: marker1, marker2, marker3, marker4
```

You must subscribe to your `Observable<MarkedQuery<M>>` before performing your marked operation
in order to observe markers.

The `SqlDim` object can also wrap a `ContentResolver` for observing a query on another app's
content provider.

```java
DimContentResolver resolver = sqlDim.wrapContentProvider(contentResolver, Schedulers.io());
Observable<Query> query = resolver.createQuery(/*...*/);
```

The full power of RxJava's operators are available for combining, filtering, and triggering any
number of queries and data changes.



Philosophy and Lineage
----------------------

SQL Dim's only responsibility is to be a mechanism for coordinating and composing the notification
of updates to tables such that you can update queries as soon as data changes.

This library is not an ORM. It is not a type-safe query mechanism. It won't serialize the same POJOs
you use for Gson. It's not going to perform database migrations for you.

Some of these features are offered by [SQL Delight][sqldelight] which can be used with SQL Dim.

SQL Dim is based on a deprecated library from Square, [SQL Brite][sqlbrite]. SQL Brite was
deprecated in favor of [SQL Delight][sqldelight], but SQL Delight requires Kotlin. This is for
people that don't want to use Kotlin.


Download
--------

```groovy
implementation 'com.stealthmountain.sqldim:sqldim:5.0.0'
```

Snapshots of the development version are available in [Sonatype's `snapshots` repository][snap].



License
-------

    Copyright 2015 Square, Inc.
    Copyright 2020 Heath Borders

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.





 [snap]: https://oss.sonatype.org/content/repositories/snapshots/
 [sqldelight]: https://github.com/square/sqldelight/
 [sqlbrite]: https://github.com/square/sqlbrite/
