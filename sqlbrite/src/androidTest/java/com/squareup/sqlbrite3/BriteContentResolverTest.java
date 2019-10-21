/*
 * Copyright (C) 2015 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.squareup.sqlbrite3;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.test.InstrumentationRegistry;
import android.test.ProviderTestCase2;
import android.test.mock.MockContentProvider;
import android.test.mock.MockContentResolver;

import com.squareup.sqlbrite3.SqlBrite.Query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.ObservableTransformer;
import io.reactivex.subjects.PublishSubject;

import static com.google.common.truth.Truth.assertThat;

public final class BriteContentResolverTest
    extends ProviderTestCase2<BriteContentResolverTest.TestContentProvider> {

  @NonNull private static final Uri AUTHORITY = Objects.requireNonNull(Uri.parse("content://test_authority"));

  @NonNull private static final Uri TABLE = AUTHORITY.buildUpon().appendPath("test_table").build();
  @NonNull private static final String KEY = "test_key";
  @NonNull private static final String VALUE = "test_value";

  @NonNull private final List<String> logs = new ArrayList<>();
  @NonNull private final RecordingObserver o = new BlockingRecordingObserver();
  @NonNull private final TestScheduler scheduler = new TestScheduler();
  @NonNull private final PublishSubject<Object> killSwitch = PublishSubject.create();

  @Nullable private ContentResolver contentResolver;
  @Nullable private BriteContentResolver db;

  public BriteContentResolverTest() {
    super(TestContentProvider.class, AUTHORITY.getAuthority());
  }

  @Override protected void setUp() throws Exception {
    super.setUp();
    @NonNull final ContentResolver contentResolver = getMockContentResolver();
    this.contentResolver = contentResolver;

    @NonNull final SqlBrite.Logger logger = new SqlBrite.Logger() {
      @Override public void log(@NonNull String message) {
        logs.add(message);
      }
    };
    @NonNull final ObservableTransformer<Query, Query> queryTransformer =
        new ObservableTransformer<Query, Query>() {
          @NonNull @Override public ObservableSource<Query> apply(@NonNull Observable<Query> upstream) {
            return upstream.takeUntil(killSwitch);
          }
        };
    db = new BriteContentResolver(this.contentResolver, logger, scheduler, queryTransformer);


    @NonNull final ContentResolver applicationContextContentResolver =
        Objects.requireNonNull(InstrumentationRegistry.getTargetContext().getContentResolver());

    getProvider().init(applicationContextContentResolver);
  }

  @Override public void tearDown() {
    o.assertNoMoreEvents();
    o.dispose();
  }

  public void testLoggerEnabled() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(true);

    db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();

    contentResolver.insert(TABLE, values("key1", "value1"));
    o.assertCursor().hasRow("key1", "value1").isExhausted();
    assertThat(logs).isNotEmpty();
  }

  public void testLoggerDisabled() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    db.setLoggingEnabled(false);

    contentResolver.insert(TABLE, values("key1", "value1"));
    assertThat(logs).isEmpty();
  }

  public void testCreateQueryObservesInsert() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertCursor().hasRow("key1", "val1").isExhausted();
  }

  public void testCreateQueryObservesUpdate() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    contentResolver.insert(TABLE, values("key1", "val1"));
    db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().hasRow("key1", "val1").isExhausted();

    contentResolver.update(TABLE, values("key1", "val2"), null, null);
    o.assertCursor().hasRow("key1", "val2").isExhausted();
  }

  public void testCreateQueryObservesDelete() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    contentResolver.insert(TABLE, values("key1", "val1"));
    db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().hasRow("key1", "val1").isExhausted();

    contentResolver.delete(TABLE, null, null);
    o.assertCursor().isExhausted();
  }

  public void testUnsubscribeDoesNotTrigger() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();
    o.dispose();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertNoMoreEvents();
    assertThat(logs).isEmpty();
  }

  public void testQueryNotNotifiedWhenQueryTransformerDisposed() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();

    killSwitch.onNext("kill");
    o.assertIsCompleted();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertNoMoreEvents();
  }

  public void testInitialValueAndTriggerUsesScheduler() {
    @NonNull final ContentResolver contentResolver = Objects.requireNonNull(this.contentResolver);
    @NonNull final BriteContentResolver db = Objects.requireNonNull(this.db);

    scheduler.runTasksImmediately(false);

    db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertCursor().isExhausted();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertCursor().hasRow("key1", "val1").isExhausted();
  }

  @NonNull private ContentValues values(@NonNull String key, @NonNull String value) {
    @NonNull final ContentValues result = new ContentValues();
    result.put(KEY, key);
    result.put(VALUE, value);
    return result;
  }

  public static final class TestContentProvider extends MockContentProvider {
    @NonNull private final Map<String, String> storage = new LinkedHashMap<>();

    @Nullable private ContentResolver contentResolver;

    void init(@NonNull ContentResolver contentResolver) {
      this.contentResolver = contentResolver;
    }

    @Nullable @Override public Uri insert(@NonNull Uri uri, @Nullable ContentValues values) {
      if (values == null) {
        return null;
      } else {
        storage.put(values.getAsString(KEY), values.getAsString(VALUE));
        Objects.requireNonNull(contentResolver).notifyChange(uri, null);
        return Uri.parse(AUTHORITY + "/" + values.getAsString(KEY));
      }
    }

    @Override public int update(@NonNull Uri uri, @Nullable ContentValues values,
        @Nullable String selection, @Nullable String[] selectionArgs) {
      if (values == null) {
        return 0;
      } else {
        for (@NonNull final String key : storage.keySet()) {
          storage.put(key, values.getAsString(VALUE));
        }
        Objects.requireNonNull(contentResolver).notifyChange(uri, null);
        return storage.size();
      }
    }

    @Override public int delete(@NonNull Uri uri,
        @Nullable String selection, @Nullable String[] selectionArgs) {
      int result = storage.size();
      storage.clear();
      Objects.requireNonNull(contentResolver).notifyChange(uri, null);
      return result;
    }

    @NonNull @Override public Cursor query(@NonNull Uri uri, @Nullable String[] projection,
        @Nullable String selection, @Nullable String[] selectionArgs, @Nullable String sortOrder) {
      @NonNull final MatrixCursor result = new MatrixCursor(new String[] { KEY, VALUE });
      for (@NonNull final Map.Entry<String, String> entry : storage.entrySet()) {
        result.addRow(new Object[] { entry.getKey(), entry.getValue() });
      }
      return result;
    }
  }
}
