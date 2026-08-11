package com.ledgerline;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Fails the build when a test class exists that surefire would not run.
 *
 * This exists because the failure it guards against has already happened
 * twice in this repository, and neither instance was caught by review,
 * compilation, or a green build:
 *
 *   - Two Day 3 sabotage classes were named {@code *Probe}, matching none of
 *     surefire's default include patterns. They compiled, they were
 *     committed, and they never executed. The build stayed green the whole
 *     time, reporting nothing about the properties they were written to
 *     prove.
 *   - A separate count discrepancy sent someone looking for exactly this
 *     mechanism on a day when it was not, in fact, the cause -- which is its
 *     own kind of cost.
 *
 * A test that never runs is worse than a missing test: a missing test is
 * visibly absent, while a silently excluded one looks like coverage. The
 * pom's {@code <includes>} list is now explicit, but an explicit list only
 * helps for names somebody thought to add. This test closes the remaining
 * gap by checking the other direction -- not "are these patterns
 * configured", but "does every test class on disk match one of them".
 *
 * Deliberately reads the source tree rather than the classpath. A class
 * excluded from surefire is still compiled and still on the classpath, so a
 * classpath scan would find it and report everything as fine; the source
 * tree is where the discrepancy is visible.
 */
class SurefireInclusionTest {

    /**
     * Mirrors the {@code <includes>} block in pom.xml. Kept as a literal list
     * rather than parsed out of the POM: parsing would make the test agree
     * with the configuration by construction, including when both are wrong
     * together, which defeats the point of having a second opinion.
     */
    private static final List<String> INCLUDED_SUFFIXES = List.of(
            "Test", "Tests", "TestCase", "Probe", "Spec", "IT");

    private static final String TEST_PREFIX = "Test";

    @Test
    void everyTestClassMatchesASurefireIncludePattern() throws IOException {
        Path testRoot = Path.of("src", "test", "java");
        List<String> unrunnable = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(testRoot)) {
            for (Path path : paths.filter(p -> p.toString().endsWith(".java")).toList()) {
                String source = Files.readString(path, StandardCharsets.UTF_8);
                if (!declaresTests(source)) {
                    continue;
                }

                String className = path.getFileName().toString().replace(".java", "");
                if (!matchesAnIncludePattern(className)) {
                    unrunnable.add(className);
                }
            }
        }

        assertThat(unrunnable)
                .as("test classes that declare @Test methods but match no surefire include "
                        + "pattern, and therefore never run: %s -- rename them or widen "
                        + "<includes> in pom.xml", unrunnable)
                .isEmpty();
    }

    /**
     * Whether the file declares JUnit tests at all. Abstract bases and plain
     * helpers legitimately match no pattern, because there is nothing in them
     * to run.
     */
    private static boolean declaresTests(String source) {
        return source.contains("@Test")
                || source.contains("@ParameterizedTest")
                || source.contains("@TestFactory");
    }

    private static boolean matchesAnIncludePattern(String className) {
        if (className.startsWith(TEST_PREFIX)) {
            return true;
        }
        return INCLUDED_SUFFIXES.stream().anyMatch(className::endsWith);
    }
}
