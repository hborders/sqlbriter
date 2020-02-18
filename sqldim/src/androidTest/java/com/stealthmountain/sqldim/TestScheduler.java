/*
 * Copyright (C) 2016 Square, Inc.
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

import androidx.annotation.NonNull;

import java.util.concurrent.TimeUnit;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;

final class TestScheduler extends Scheduler {
  @NonNull private final io.reactivex.rxjava3.schedulers.TestScheduler delegate =
      new io.reactivex.rxjava3.schedulers.TestScheduler();

  private boolean runTasksImmediately = true;

  public void runTasksImmediately(boolean runTasksImmediately) {
    this.runTasksImmediately = runTasksImmediately;
  }

  public void triggerActions() {
    delegate.triggerActions();
  }

  @NonNull @Override public Worker createWorker() {
    return new TestWorker();
  }

  class TestWorker extends Worker {
    @NonNull private final Worker delegateWorker = delegate.createWorker();

    @NonNull @Override
    public Disposable schedule(@NonNull Runnable run, long delay, @NonNull TimeUnit unit) {
      @NonNull final Disposable disposable = delegateWorker.schedule(run, delay, unit);
      if (runTasksImmediately) {
        triggerActions();
      }
      return disposable;
    }

    @Override public void dispose() {
      delegateWorker.dispose();
    }

    @Override public boolean isDisposed() {
      return delegateWorker.isDisposed();
    }
  }
}
