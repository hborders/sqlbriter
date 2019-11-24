package com.stealthmountain.sqldim;

import com.android.annotations.NonNull;
import com.android.tools.lint.checks.infrastructure.LintDetectorTest;

import org.junit.Test;

import static com.android.tools.lint.checks.infrastructure.TestFiles.java;
import static com.android.tools.lint.checks.infrastructure.TestLintTask.lint;

public final class SqlDimArgCountDetectorTest {
    @SuppressWarnings("UnstableApiUsage")
    @NonNull private static final LintDetectorTest.TestFile DIM_DATABASE_STUB =
            java(
                    "package com.stealthmountain.sqldim;\n" +
                            "\n" +
                            "public final class DimDatabase<M> {\n" +
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
                DIM_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.stealthmountain.sqldim.DimDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      DimDatabase<Object> db = new DimDatabase<>();\n" +
                                "      db.query(QUERY, \"id\");\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlDimArgCountDetector.ISSUE)
                .run()
                .expectClean();
    }

    @Test
    public void cleanCaseWithQueryThatCantBeEvaluated() {
        lint().files(
                DIM_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.stealthmountain.sqldim.DimDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      DimDatabase<Object> db = new DimDatabase<>();\n" +
                                "      db.query(query(), \"id\");\n" +
                                "    }\n" +
                                "    private String query() {\n" +
                                "      return QUERY + \" age = ?\";\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                        )
        )
                .issues(SqlDimArgCountDetector.ISSUE)
                .run()
                .expectClean();
    }

    @Test
    public void cleanCaseWithNonVarargMethodCall() {
        lint().files(
                DIM_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.stealthmountain.sqldim.DimDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      DimDatabase<Object> db = new DimDatabase<>();\n" +
                                "      db.createQuery(\"table\", 42);\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlDimArgCountDetector.ISSUE)
                .run()
                .expectClean();
    }

    @Test
    public void queryMethodWithWrongNumberOfArguments() {
        lint().files(
                DIM_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.stealthmountain.sqldim.DimDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      DimDatabase<Object> db = new DimDatabase<>();\n" +
                                "      db.query(QUERY);\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlDimArgCountDetector.ISSUE)
                .run()
                .expect(
                        "src/test/pkg/Test.java:10: " +
                                "Error: Wrong argument count, query SELECT name FROM table WHERE id = ?" +
                                " requires 1 argument, but was provided 0 arguments [SqlDimArgCount]\n" +
                                "      db.query(QUERY);\n" +
                                "      ~~~~~~~~~~~~~~~\n" +
                                "1 errors, 0 warnings"
                );
    }

    @Test
    public void createQueryMethodWithWrongNumberOfArguments() {
        lint().files(
                DIM_DATABASE_STUB,
                java(
                        "package test.pkg;\n" +
                                "\n" +
                                "import com.stealthmountain.sqldim.DimDatabase;\n" +
                                "\n" +
                                "public class Test {\n" +
                                "    private static final String QUERY = \"SELECT name FROM table WHERE id = ?\";\n" +
                                "\n" +
                                "    public void test() {\n" +
                                "      DimDatabase<Object> db = new DimDatabase<>();\n" +
                                "      db.createQuery(\"table\", QUERY);\n" +
                                "    }\n" +
                                "\n" +
                                "}\n"
                )
        )
                .issues(SqlDimArgCountDetector.ISSUE)
                .run()
                .expect(
                        "src/test/pkg/Test.java:10: " +
                                "Error: Wrong argument count, query SELECT name FROM table WHERE id = ?" +
                                " requires 1 argument, but was provided 0 arguments [SqlDimArgCount]\n" +
                                "      db.createQuery(\"table\", QUERY);\n" +
                                "      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
                                "1 errors, 0 warnings"
                );
    }
}
