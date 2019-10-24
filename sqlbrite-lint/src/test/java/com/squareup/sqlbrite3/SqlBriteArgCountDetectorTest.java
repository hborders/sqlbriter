package com.squareup.sqlbrite3;

import com.android.annotations.NonNull;
import com.android.tools.lint.checks.infrastructure.LintDetectorTest;

import org.junit.Test;

import static com.android.tools.lint.checks.infrastructure.TestFiles.java;
import static com.android.tools.lint.checks.infrastructure.TestLintTask.lint;

public final class SqlBriteArgCountDetectorTest {
    @SuppressWarnings("UnstableApiUsage")
    @NonNull private static final LintDetectorTest.TestFile BRITE_DATABASE_STUB =
            java(
                    "package com.squareup.sqlbrite3;\n" +
                            "\n" +
                            "public final class BriteDatabase {\n" +
                            "\n" +
                            "  public void query(String sql, Object... args) {\n" +
                            "  }\n" +
                            "  \n" +
                            "  public void createQuery(String table, String sql, Object... args) {\n" +
                            "  }\n" +
                            "  \n" +
                            "  // simulate createQuery with SupportSQLiteQuery query parameter\n" +
                            "  public void createQuery(String table, int something) {\n" +
                            "  }\n" +
                            "}\n"
            );

    @Test
    public void cleanCaseWithWithQueryAsLiteral() {
        lint().files(
                BRITE_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.squareup.sqlbrite3.BriteDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      BriteDatabase db = new BriteDatabase();\n" +
                                "      db.query(QUERY, \"id\");\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlBriteArgCountDetector.ISSUE)
                .run()
                .expectClean();
    }

    @Test
    public void cleanCaseWithQueryThatCantBeEvaluated() {
        lint().files(
                BRITE_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.squareup.sqlbrite3.BriteDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      BriteDatabase db = new BriteDatabase();\n" +
                                "      db.query(query(), \"id\");\n" +
                                "    }\n" +
                                "    private String query() {\n" +
                                "      return QUERY + \" age = ?\";\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                        )
        )
                .issues(SqlBriteArgCountDetector.ISSUE)
                .run()
                .expectClean();
    }

    @Test
    public void cleanCaseWithNonVarargMethodCall() {
        lint().files(
                BRITE_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.squareup.sqlbrite3.BriteDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      BriteDatabase db = new BriteDatabase();\n" +
                                "      db.createQuery(\"table\", 42);\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlBriteArgCountDetector.ISSUE)
                .run()
                .expectClean();
    }

    @Test
    public void queryMethodWithWrongNumberOfArguments() {
        lint().files(
                BRITE_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.squareup.sqlbrite3.BriteDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      BriteDatabase db = new BriteDatabase();\n" +
                                "      db.query(QUERY);\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlBriteArgCountDetector.ISSUE)
                .run()
                .expect(
                        "src/test/pkg/Test.java:10: " +
                                "Error: Wrong argument count, query SELECT name FROM table WHERE id = ?" +
                                " requires 1 argument, but was provided 0 arguments [SqlBriteArgCount]\n" +
                                "      db.query(QUERY);\n" +
                                "      ~~~~~~~~~~~~~~~\n" +
                                "1 errors, 0 warnings"
                );
    }

    @Test
    public void createQueryMethodWithWrongNumberOfArguments() {
        lint().files(
                BRITE_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.squareup.sqlbrite3.BriteDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      BriteDatabase db = new BriteDatabase();\n" +
                                "      db.createQuery(\"table\", QUERY);\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlBriteArgCountDetector.ISSUE)
                .run()
                .expect(
                        "src/test/pkg/Test.java:10: " +
                                "Error: Wrong argument count, query SELECT name FROM table WHERE id = ?" +
                                " requires 1 argument, but was provided 0 arguments [SqlBriteArgCount]\n" +
                                "      db.createQuery(\"table\", QUERY);\n" +
                                "      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
                                "1 errors, 0 warnings"
                );
    }
}
