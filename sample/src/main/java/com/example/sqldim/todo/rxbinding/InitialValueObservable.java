package com.example.sqldim.todo.rxbinding;

import androidx.annotation.NonNull;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;

// Replaced RxBinding usage until it supports RxJava3
// https://github.com/JakeWharton/RxBinding/issues/531
public abstract class InitialValueObservable<T> extends Observable<T> {
  @NonNull protected abstract T initialValue();

  @Override
  protected void subscribeActual(@NonNull Observer<? super T> observer) {
    subscribeListener(observer);
    observer.onNext(initialValue());
  }

  protected abstract void subscribeListener(@NonNull Observer<? super T> observer);

  public final Observable<T> skipInitialValue() {
    return new Skipped();
  }

  private final class Skipped extends Observable<T> {
    @Override
    protected void subscribeActual(@NonNull Observer<? super T> observer) {
      subscribeListener(observer);
    }
  }
}
