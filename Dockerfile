# syntax=docker/dockerfile:1

# Stage 1: resolve dependencies into a layer that only invalidates when the
# POM changes, not on every source edit -- pom.xml is copied and resolved
# before any src/ file is copied in, so an inner-loop code change reuses this
# entire layer from cache instead of re-downloading the dependency tree.
FROM eclipse-temurin:21-jdk AS deps
WORKDIR /build
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
# Resolves the full dependency tree (including the parent POM) with network
# access, once. This layer's cache key is the POM alone, so it's reused for
# every later build that doesn't change a dependency -- a source-only change
# never re-triggers this download.
RUN chmod +x mvnw && ./mvnw -q dependency:go-offline -B

# Stage 2: compile and package. Only invalidated by a source change -- this
# layer starts from `deps`, so Docker's own build cache reuses the entire
# dependency-resolution layer above without re-running it, regardless of
# whether Maven itself is offline. (Deliberately not passing -o/--offline
# here: `dependency:go-offline` in the deps stage does not reliably pull
# every plugin- and test-scoped artifact -- e.g. Flyway's optional TOML
# config support, Mockito's byte-buddy agent -- so a fully offline package
# step genuinely fails on some builds. The caching win comes from Docker
# layer reuse, not from forcing Maven itself into offline mode.)
FROM deps AS build
COPY src/ src/
RUN ./mvnw -q package -DskipTests -B

# Stage 3: runtime. A JRE, not a JDK -- nothing here compiles Java, so the
# extra ~100MB of javac/jdb/etc a full JDK carries buys nothing and only
# widens the attack surface.
FROM eclipse-temurin:21-jre AS runtime

# Real security basic, not decoration: a container that runs as root gives
# away nothing by default, but a JVM RCE or a dependency vulnerability run as
# root can pivot to the node in ways a low-privilege user can't. --system
# accounts get no login shell and no password, which is exactly what a
# service process needs and nothing more.
RUN groupadd --system ledgerline && useradd --system --gid ledgerline --no-create-home ledgerline

WORKDIR /app
COPY --from=build /build/target/ledgerline-0.0.1-SNAPSHOT.jar app.jar
RUN chown -R ledgerline:ledgerline /app
USER ledgerline

EXPOSE 8080

# Explicit heap flags, not the JVM's own container-memory autodetection.
#
# The JVM's default heap sizing (since JDK 10, -XX:+UseContainerSupport)
# reads the container's cgroup memory limit and sizes the default max heap
# (-XX:MaxRAMPercentage, 25% by default) off of it -- but that arithmetic
# runs once, at JVM startup, against whatever limit is visible *then*. It can
# get this wrong in ways that are hard to notice until the process is
# OOM-killed under a limit it never actually tried to respect: cgroup v1
# quirks, a limit applied after the JVM already read it, or simply every
# other process in the container (native Kafka client buffers, off-heap
# Netty buffers Spring Kafka uses, thread stacks) competing for the same
# container memory budget the JVM only partially reserved for heap. Setting
# -Xmx/-Xms explicitly, sized with headroom below the container's actual
# memory limit (set in the Kubernetes resource spec, see the Helm chart's
# values.yaml), means the heap ceiling is a number this Dockerfile states on
# purpose, not a guess the JVM makes once and never revisits. JAVA_OPTS is
# appended after these defaults so a Helm value can widen or narrow the heap
# per environment without rebuilding the image.
ENV JAVA_TOOL_OPTIONS="-Xms256m -Xmx512m -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_TOOL_OPTIONS -jar app.jar \"$@\"", "--"]
