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
package com.stealthmountain.sqldim;

import android.database.Cursor;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import io.reactivex.observers.DisposableObserver;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.stealthmountain.sqldim.SqlDim.MarkedQuery;

class MarkedRecordingObserver extends DisposableObserver<MarkedQuery<String>> {
  @NonNull private static final Object COMPLETED = "<completed>";
  @NonNull private static final String TAG = MarkedRecordingObserver.class.getSimpleName();

  @NonNull final BlockingDeque<Object> events = new LinkedBlockingDeque<>();

  @Override public final void onComplete() {
    Log.d(TAG, "onCompleted");
    events.add(COMPLETED);
  }

  @Override public final void onError(@NonNull Throwable e) {
    Log.d(TAG, "onError " + e.getClass().getSimpleName() + " " + e.getMessage());
    events.add(e);
  }

  @Override public final void onNext(@NonNull MarkedQuery<String> value) {
    Log.d(TAG, "onNext " + value);
    events.add(value.markers);
    events.add(value.run());
  }

  @NonNull protected Object takeEvent() {
    @Nullable final Object item = events.removeFirst();
    if (item == null) {
      throw new AssertionError("No items.");
    }
    return item;
  }

  public final void assertMarkersEquals(@NonNull String... expected) {
    @NonNull final Object event = takeEvent();
    assertThat(event).isInstanceOf(Set.class);
    assertThat((Set<?>) event).isEqualTo(new HashSet<>(Arrays.asList(expected)));
  }

  public final void assertEmptyMarkers() {
    @NonNull final Object event = takeEvent();
    assertThat(event).isInstanceOf(Set.class);
    assertThat((Set<?>) event).isEmpty();
  }

  public final CursorAssert assertCursor() {
    @NonNull final Object event = takeEvent();
    assertThat(event).isInstanceOf(Cursor.class);
    return new CursorAssert((Cursor) event);
  }

  public final void assertErrorContains(String expected) {
    @NonNull final Object event = takeEvent();
    assertThat(event).isInstanceOf(Throwable.class);
    assertThat(((Throwable) event).getMessage()).contains(expected);
  }

  public final void assertIsCompleted() {
    @NonNull final Object event = takeEvent();
    assertThat(event).isEqualTo(COMPLETED);
  }

  public void assertNoMoreEvents() {
    assertThat(events).isEmpty();
  }

  static final class CursorAssert {
    @NonNull private final Cursor cursor;
    private int row = 0;

    CursorAssert(@NonNull Cursor cursor) {
      this.cursor = cursor;
    }

    @NonNull
    public CursorAssert hasRow(@NonNull Object... values) {
      assertWithMessage("row " + (row + 1) + " exists")
          .that(cursor.moveToNext()).isTrue();
      row += 1;
      assertWithMessage("column count")
          .that(cursor.getColumnCount()).isEqualTo(values.length);
      for (int i = 0; i < values.length; i++) {
        assertWithMessage("row " + row + " column '" + cursor.getColumnName(i) + "'")
            .that(cursor.getString(i))
            .isEqualTo(values[i]);
      }
      return this;
    }

    public void isExhausted() {
      if (cursor.moveToNext()) {
        @NonNull final StringBuilder data = new StringBuilder();
        for (int i = 0; i < cursor.getColumnCount(); i++) {
          if (i > 0) data.append(", ");
          data.append(cursor.getString(i));
        }
        throw new AssertionError("Expected no more rows but was: " + data);
      }
      cursor.close();
    }
  }
}
