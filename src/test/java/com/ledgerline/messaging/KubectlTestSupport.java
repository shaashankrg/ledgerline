package com.ledgerline.messaging;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared {@code kubectl} process-management helpers for tests that drive a
 * real, live {@code kind} cluster ({@link ChaosInvariantTest}, {@link
 * AutoCommitLossMeasurementTest}) -- extracted rather than duplicated once a
 * second test needed the exact same port-forward/exec/delete-pod mechanics.
 */
final class KubectlTestSupport {

    private static final Logger log = LoggerFactory.getLogger(KubectlTestSupport.class);

    private KubectlTestSupport() {
    }

    static Process startPortForward(String namespace, String target, int remotePort, int localPort)
            throws IOException {
        List<String> command = List.of("kubectl", "port-forward", "-n", namespace, target,
                localPort + ":" + remotePort);
        log.info("starting {}", String.join(" ", command));
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(new File(System.getProperty("java.io.tmpdir"),
                        "ledgerline-portforward-" + remotePort + "-" + System.currentTimeMillis() + ".log"))
                .start();
    }

    static void awaitPortOpen(String host, int port, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 1000);
                return;
            } catch (IOException e) {
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("port-forward to " + host + ":" + port + " never became reachable");
    }

    static String runKubectl(String... args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add("kubectl");
        command.addAll(List.of(args));

        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = readAll(process);
        boolean finished = process.waitFor(60, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IllegalStateException("kubectl command timed out: " + command);
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("kubectl command failed (" + process.exitValue() + "): "
                    + command + "\n" + output);
        }
        return output;
    }

    static List<String> listPods(String namespace, String label) throws Exception {
        String output = runKubectl("get", "pods", "-n", namespace, "-l", label,
                "--field-selector=status.phase=Running",
                "-o", "jsonpath={.items[*].metadata.name}");
        return output.isBlank() ? List.of() : List.of(output.trim().split("\\s+"));
    }

    static void deletePod(String namespace, String podName) throws Exception {
        runKubectl("delete", "pod", "-n", namespace, podName, "--wait=false");
    }

    private static String readAll(Process process) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }
}
