/*
 * Copyright (C) 2017 Square, Inc.
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

import android.database.Cursor;
import android.os.Build;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.RequiresApi;

import java.util.Optional;
import java.util.Set;

import io.reactivex.ObservableOperator;
import io.reactivex.Observer;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.BiFunction;
import io.reactivex.observers.DisposableObserver;
import io.reactivex.plugins.RxJavaPlugins;

import static com.squareup.sqlbrite3.SqlBrite.MarkedQuery;
import static com.squareup.sqlbrite3.SqlBrite.MarkedQuery.MarkedValue;

@RequiresApi(Build.VERSION_CODES.N)
final class MarkedQueryToOptionalOperator<M, T>
        implements ObservableOperator<MarkedValue<M, Optional<T>>, MarkedQuery<M>> {
  @NonNull private final BiFunction<Cursor, Set<M>, T> mapper;

  MarkedQueryToOptionalOperator(@NonNull BiFunction<Cursor, Set<M>, T> mapper) {
    this.mapper = mapper;
  }

  @NonNull @Override
  public Observer<? super MarkedQuery<M>> apply(
          @NonNull Observer<? super MarkedValue<M, Optional<T>>> observer) {
    return new MappingObserver<>(observer, mapper);
  }

  static final class MappingObserver<M, T> extends DisposableObserver<MarkedQuery<M>> {
    @NonNull private final Observer<? super MarkedValue<M, Optional<T>>> downstream;
    @NonNull private final BiFunction<Cursor, Set<M>, T> mapper;

    MappingObserver(@NonNull Observer<? super MarkedValue<M, Optional<T>>> downstream,
                    @NonNull BiFunction<Cursor, Set<M>, T> mapper) {
      this.downstream = downstream;
      this.mapper = mapper;
    }

    @Override protected void onStart() {
      downstream.onSubscribe(this);
    }

    @Override public void onNext(@NonNull MarkedQuery<M> markedQuery) {
      try {
        @Nullable final T item;
        @NonNull final Set<M> markers = markedQuery.markers;
        @Nullable final Cursor cursor = markedQuery.run();
        if (cursor != null) {
          try {
            if (cursor.moveToNext()) {
              item = mapper.apply(cursor, markers);
              // even though the type system should make this impossible,
              // Java doesn't always check nullability annotations,
              // so leave this in just in case our clients don't follow the rules.
              if (item == null) {
                downstream.onError(new NullPointerException("QueryToOne mapper returned null"));
                return;
              }
              if (cursor.moveToNext()) {
                throw new IllegalStateException("Cursor returned more than 1 row");
              }
            } else {
              item = null;
            }
          } finally {
            cursor.close();
          }
        } else {
          item = null;
        }
        if (!isDisposed()) {
          downstream.onNext(new MarkedValue<>(markers, Optional.ofNullable(item)));
        }
      } catch (Throwable e) {
        Exceptions.throwIfFatal(e);
        onError(e);
      }
    }

    @Override public void onComplete() {
      if (!isDisposed()) {
        downstream.onComplete();
      }
    }

    @Override public void onError(@NonNull Throwable e) {
      if (isDisposed()) {
        RxJavaPlugins.onError(e);
      } else {
        downstream.onError(e);
      }
    }
  }
}
