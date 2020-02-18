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
package com.stealthmountain.sqldim;

import android.database.Cursor;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.util.List;
import java.util.Set;

import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.functions.BiFunction;
import io.reactivex.rxjava3.observers.DisposableObserver;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;

import static com.stealthmountain.sqldim.SqlDim.MarkedQuery;
import static com.stealthmountain.sqldim.SqlDim.MarkedQuery.MarkedValue;

final class MarkedQueryToListOperator<M, L extends List<T>, T> implements ObservableOperator<MarkedValue<M, L>, MarkedQuery<M>> {
  @NonNull private final BiFunction<Cursor, Set<M>, T> mapper;
  @NonNull private final NewList<L, T> newList;

  MarkedQueryToListOperator(@NonNull BiFunction<Cursor, Set<M>, T> mapper, @NonNull NewList<L, T> newList) {
    this.mapper = mapper;
    this.newList = newList;
  }

  @NonNull @Override
  public Observer<? super MarkedQuery<M>> apply(@NonNull Observer<? super MarkedValue<M, L>> observer) {
    return new MappingObserver<>(observer, mapper, newList);
  }

  static final class MappingObserver<M, L extends List<T>, T> extends DisposableObserver<MarkedQuery<M>> {
    @NonNull private final Observer<? super MarkedValue<M, L>> downstream;
    @NonNull private final BiFunction<Cursor, Set<M>, T> mapper;
    @NonNull private final NewList<L, T> newList;

    MappingObserver(@NonNull Observer<? super MarkedValue<M, L>> downstream,
                    @NonNull BiFunction<Cursor, Set<M>, T> mapper, @NonNull NewList<L, T> newList) {
      this.downstream = downstream;
      this.mapper = mapper;
      this.newList = newList;
    }

    @Override protected void onStart() {
      downstream.onSubscribe(this);
    }

    @Override public void onNext(@NonNull MarkedQuery<M> markedQuery) {
      try {
        @Nullable T item;
        @Nullable final Cursor cursor = markedQuery.run();
        if (cursor == null || isDisposed()) {
          return;
        }
        @NonNull final L items = newList.newList(cursor.getCount());
        @NonNull final Set<M> markers = markedQuery.markers;
        try {
          while (cursor.moveToNext()) {
            item = mapper.apply(cursor, markers);
            // even though the type system should make this impossible,
            // Java doesn't always check nullability annotations,
            // so leave this in just in case our clients don't follow the rules.
            if (item == null) {
              downstream.onError(new NullPointerException("QueryToList mapper returned null"));
              return;
            }
            items.add(item);
          }
        } finally {
          cursor.close();
        }
        if (!isDisposed()) {
          downstream.onNext(new MarkedValue<>(markers, items));
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
