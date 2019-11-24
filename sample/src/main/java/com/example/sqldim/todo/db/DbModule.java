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
package com.example.sqldim.todo.db;

import android.app.Application;

import androidx.annotation.NonNull;
import androidx.sqlite.db.SupportSQLiteOpenHelper;
import androidx.sqlite.db.SupportSQLiteOpenHelper.Configuration;
import androidx.sqlite.db.SupportSQLiteOpenHelper.Factory;
import androidx.sqlite.db.framework.FrameworkSQLiteOpenHelperFactory;
import com.squareup.sqlbrite3.BriteDatabase;
import com.squareup.sqlbrite3.SqlBrite;
import dagger.Module;
import dagger.Provides;
import io.reactivex.schedulers.Schedulers;
import javax.inject.Singleton;
import timber.log.Timber;

@Module
public final class DbModule {
  @NonNull @Provides @Singleton SqlBrite<Object> provideSqlBrite() {
    return new SqlBrite.Builder<Object>()
        .logger(new SqlBrite.Logger() {
          @Override public void log(@NonNull String message) {
            Timber.tag("Database").v(message);
          }
        })
        .build();
  }

    @NonNull @Provides @Singleton
    BriteDatabase<Object> provideDatabase(@NonNull SqlBrite<Object> sqlBrite,
                                          @NonNull Application application) {
    Configuration configuration = Configuration.builder(application)
        .name("todo.db")
        .callback(new DbCallback())
        .build();
    Factory factory = new FrameworkSQLiteOpenHelperFactory();
    SupportSQLiteOpenHelper helper = factory.create(configuration);
    BriteDatabase<Object> db = sqlBrite.wrapDatabaseHelper(helper, Schedulers.io());
    db.setLoggingEnabled(true);
    return db;
  }
}
