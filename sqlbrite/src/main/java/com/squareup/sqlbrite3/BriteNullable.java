package com.squareup.sqlbrite3;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.LOCAL_VARIABLE;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.CLASS;

/**
 * A {@link androidx.annotation.Nullable} doesn't apply to {@link java.lang.annotation.ElementType#TYPE_USE},
 * so we have to provide this annotation so that {@link BriteOptional} can have a properly-annotated
 * generic type without requiring all the baggage of the checker framework.
 */
@Documented
@Retention(CLASS)
@Target({METHOD, PARAMETER, FIELD, LOCAL_VARIABLE, ANNOTATION_TYPE, PACKAGE, TYPE_USE})
@interface BriteNullable {
}
