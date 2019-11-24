package com.stealthmountain.sqldim;

import com.android.annotations.NonNull;
import com.android.annotations.Nullable;
import com.android.tools.lint.client.api.JavaEvaluator;
import com.android.tools.lint.detector.api.Category;
import com.android.tools.lint.detector.api.Detector;
import com.android.tools.lint.detector.api.Implementation;
import com.android.tools.lint.detector.api.Issue;
import com.android.tools.lint.detector.api.JavaContext;
import com.android.tools.lint.detector.api.Scope;
import com.android.tools.lint.detector.api.Severity;
import com.intellij.psi.PsiMethod;

import org.jetbrains.uast.UCallExpression;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static com.android.tools.lint.detector.api.ConstantEvaluator.evaluateString;

@SuppressWarnings("UnstableApiUsage")
public final class SqlBriteArgCountDetector extends Detector implements Detector.UastScanner {
    @NonNull public static final Issue ISSUE = Issue.create(
            "SqlBriteArgCount",
            "Number of provided arguments doesn't match number " +
                    "of arguments specified in query",
            "When providing arguments to query you need to provide the same amount of " +
                    "arguments that is specified in query.",
            Category.MESSAGES,
            9,
            Severity.ERROR,
            new Implementation(
                    SqlBriteArgCountDetector.class,
                    EnumSet.of(
                            Scope.JAVA_FILE,
                            Scope.TEST_SOURCES
                    )
            )
    );

    @NonNull private static final String BRITE_DATABASE = "com.stealthmountain.sqldim.BriteDatabase";
    @NonNull private static final String QUERY_METHOD_NAME = "query";
    @NonNull private static final String CREATE_QUERY_METHOD_NAME = "createQuery";

    @NonNull
    @Override
    public List<String> getApplicableMethodNames() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        CREATE_QUERY_METHOD_NAME,
                        QUERY_METHOD_NAME
                )
        );
    }

    @Override
    public void visitMethodCall(
            @NonNull JavaContext context,
            @NonNull UCallExpression call,
            @NonNull PsiMethod method
    ) {
        @NonNull final JavaEvaluator evaluator = context.getEvaluator();
        if (evaluator.isMemberInClass(method, BRITE_DATABASE)) {
            // Skip non varargs overloads.
            if (!method.isVarArgs()) return;

            // Position of sql parameter depends on method.
            @Nullable final String sql = evaluateString(
                    context,
                    call.getValueArguments().get(
                            isQueryMethod(call) ? 0 : 1
                    ),
                    true
            );
            if (sql == null) {
                return;
            }

            final int argumentsCount =
                    call.getValueArgumentCount() - (isQueryMethod(call) ? 1 : 2);
            final int questionMarksCount = count(sql, '?');
            if (argumentsCount != questionMarksCount) {
                @NonNull final String requiredArguments =
                        questionMarksCount + " " + pluralize(
                                "argument",
                                questionMarksCount
                        );
                @NonNull final String actualArguments =
                        argumentsCount + " " + pluralize(
                                "argument",
                                argumentsCount
                        );
                context.report(
                        ISSUE,
                        call,
                        context.getLocation(call),
                        String.format(
                                "Wrong argument count, query %s requires %s, but was provided %s",
                                sql,
                                requiredArguments,
                                actualArguments
                        )
                );
            }
        }
    }

    private static boolean isQueryMethod(@NonNull UCallExpression call) {
        return QUERY_METHOD_NAME.equals(call.getMethodName());
    }

    private static int count(@NonNull String source, char character) {
        int count = 0;
        for (int i = 0, l = source.length(); i < l; i++) {
            final char element = source.charAt(i);
            if (element == character) {
                count++;
            }
        }
        return count;
    }

    @NonNull
    private static String pluralize(@NonNull String source, int count) {
        if (count == 1) {
            return source;
        } else {
            return source + "s";
        }
    }
}
