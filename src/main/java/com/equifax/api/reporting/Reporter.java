package com.equifax.api.reporting;

import com.equifax.api.core.common.CommonAPI;

public class Reporter {

    private static Reporter report = new Reporter();

    private Reporter() {
    }

    public static Reporter getInstance() {
        return report;
    }

}
