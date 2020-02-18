package com.stealthmountain.sqldim;

import androidx.annotation.NonNull;

import java.util.Objects;

import io.reactivex.rxjava3.functions.Predicate;

final class AssertErrorMessagePredicate implements Predicate<Throwable> {
  @NonNull private final String expectedErrorMessage;

  static Predicate<Throwable> assertErrorMessage(@NonNull String expectedErrorMessage) {
      return new AssertErrorMessagePredicate(expectedErrorMessage);
  }

  private AssertErrorMessagePredicate(@NonNull String expectedErrorMessage) {
    this.expectedErrorMessage = expectedErrorMessage;
  }

    @Override
    public boolean test(Throwable throwable) {
      return Objects.equals(expectedErrorMessage, throwable.getMessage());
    }
}
