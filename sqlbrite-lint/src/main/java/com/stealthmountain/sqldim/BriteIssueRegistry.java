package com.stealthmountain.sqldim;

import com.android.annotations.NonNull;
import com.android.tools.lint.client.api.IssueRegistry;
import com.android.tools.lint.detector.api.Issue;

import java.util.Collections;
import java.util.List;

@SuppressWarnings("UnstableApiUsage")
public final class BriteIssueRegistry extends IssueRegistry {

    @Override public int getApi() {
        return com.android.tools.lint.detector.api.ApiKt.CURRENT_API;
    }

    @NonNull
    @Override
    public List<Issue> getIssues() {
        return Collections.singletonList(SqlBriteArgCountDetector.ISSUE);
    }
}
