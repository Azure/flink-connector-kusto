package com.microsoft.azure.flink.archtests;

import org.apache.flink.architecture.common.ImportOptions;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

/** Architecture tests for test code. */
@AnalyzeClasses(
        packages = "com.microsoft.azure",
        importOptions = {
                ImportOption.OnlyIncludeTests.class,
                ImportOptions.ExcludeScalaImportOption.class,
                ImportOptions.ExcludeShadedImportOption.class
        })
public class TestKustoFlinkArchitectureCodeTest {

    @ArchTest
    public static final ArchTests COMMON_TESTS = ArchTests.in(TestKustoFlinkArchitectureCodeTest.class);
}
