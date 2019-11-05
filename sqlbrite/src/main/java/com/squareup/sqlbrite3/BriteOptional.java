package com.squareup.sqlbrite3;


import androidx.annotation.NonNull;

import java.util.Objects;

/**
 * Stripped down version of {@link java.util.Optional} for internal use only
 * that won't cause API warnings. We need this because Android can't use type-level
 * nullability annotations yet. See {@link BriteDatabase#transactions}
 */
final class BriteOptional<@BriteNullable T> {
    /**
     * Common instance for {@code empty()}.
     */
    @NonNull private static final BriteOptional<?> EMPTY = new BriteOptional<>();

    /**
     * If non-null, the value; if null, indicates no value is present
     */
    @BriteNullable private final T value;

    /**
     * Constructs an empty instance.
     *
     * @implNote Generally only one empty instance, {@link BriteOptional#EMPTY},
     * should exist per VM.
     */
    private BriteOptional() {
        this.value = null;
    }

    /**
     * Returns an empty {@code Optional} instance.  No value is present for this
     * Optional.
     *
     * @apiNote Though it may be tempting to do so, avoid testing if an object
     * is empty by comparing with {@code ==} against instances returned by
     * {@code Option.empty()}. There is no guarantee that it is a singleton.
     * Instead, use {@link #isPresent()}.
     *
     * @param <T> Type of the non-existent value
     * @return an empty {@code Optional}
     */
    @NonNull
    public static<T> BriteOptional<T> empty() {
        @SuppressWarnings("unchecked")
        BriteOptional<T> t = (BriteOptional<T>) EMPTY;
        return t;
    }

    /**
     * Constructs an instance with the value present.
     *
     * @param value the non-null value to be present
     * @throws NullPointerException if value is null
     */
    private BriteOptional(T value) {
        this.value = Objects.requireNonNull(value);
    }

    /**
     * Returns an {@code Optional} with the specified present non-null value.
     *
     * @param <T> the class of the value
     * @param value the value to be present, which must be non-null
     * @return an {@code Optional} with the value present
     * @throws NullPointerException if value is null
     */
    @NonNull
    public static <T> BriteOptional<T> of(@NonNull T value) {
        return new BriteOptional<>(value);
    }

    /**
     * Returns an {@code Optional} describing the specified value, if non-null,
     * otherwise returns an empty {@code Optional}.
     *
     * @param <T> the class of the value
     * @param value the possibly-null value to describe
     * @return an {@code Optional} with a present value if the specified value
     * is non-null, otherwise an empty {@code Optional}
     */
    @NonNull
    public static <T> BriteOptional<T> ofNullable(@BriteNullable T value) {
        return value == null ? empty() : of(value);
    }

    /**
     * Return the value if present, otherwise return {@code other}.
     *
     * @param optional the optional to check for a value, may be null.
     * @param other the value to be returned if there is no value present or optional is null, may
     * be null
     * @return the value, if present, otherwise {@code other}
     */
    @BriteNullable
    public static <T> T orElse(@BriteNullable BriteOptional<T> optional, @BriteNullable T other) {
        if (optional == null) {
            return other;
        } else {
            return optional.orElse(other);
        }
    }

    /**
     * Return {@code true} if there is a value present, otherwise {@code false}.
     *
     * @param optional the optional to check for a value, may be null.
     *
     * @return {@code true} if there is a value present, otherwise {@code false}
     */
    public static <T> boolean isPresent(@BriteNullable BriteOptional<T> optional) {
        if (optional == null) {
            return false;
        } else {
            return optional.isPresent();
        }
    }

    /**
     * Return {@code true} if there is a value present, otherwise {@code false}.
     *
     * @return {@code true} if there is a value present, otherwise {@code false}
     */
    public boolean isPresent() {
        return value != null;
    }

    /**
     * Return the value if present, otherwise return {@code other}.
     *
     * @param other the value to be returned if there is no value present, may
     * be null
     * @return the value, if present, otherwise {@code other}
     */
    @BriteNullable
    public T orElse(@BriteNullable T other) {
        return value != null ? value : other;
    }

    /**
     * Indicates whether some other object is "equal to" this Optional. The
     * other object is considered equal if:
     * <ul>
     * <li>it is also an {@code Optional} and;
     * <li>both instances have no value present or;
     * <li>the present values are "equal to" each other via {@code equals()}.
     * </ul>
     *
     * @param obj an object to be tested for equality
     * @return {code true} if the other object is "equal to" this object
     * otherwise {@code false}
     */
    @Override
    public boolean equals(@BriteNullable Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof BriteOptional)) {
            return false;
        }

        @NonNull BriteOptional<?> other = (BriteOptional<?>) obj;
        return (value == other.value) || (value != null && value.equals(other.value));
    }

    /**
     * Returns the hash code value of the present value, if any, or 0 (zero) if
     * no value is present.
     *
     * @return hash code value of the present value or 0 if no value is present
     */
    @Override
    public int hashCode() {
        return value == null ? 0 : value.hashCode();
    }

    /**
     * Returns a non-empty string representation of this Optional suitable for
     * debugging. The exact presentation format is unspecified and may vary
     * between implementations and versions.
     *
     * @implSpec If a value is present the result must include its string
     * representation in the result. Empty and present Optionals must be
     * unambiguously differentiable.
     *
     * @return the string representation of this instance
     */
    @NonNull
    @Override
    public String toString() {
        return value != null
                ? String.format("BriteOptional[%s]", value)
                : "BriteOptional.empty";
    }
}
