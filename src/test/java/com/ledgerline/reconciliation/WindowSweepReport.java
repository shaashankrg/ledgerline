package com.ledgerline.reconciliation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import com.ledgerline.settlement.NetworkFaultType;

/**
 * Writes the window-sweep deliverable Day 4 asks for: a raw CSV (one row per
 * window x fault type x seed) and a small self-contained SVG line chart
 * (overall precision/recall vs. window), both saved into the repo rather than
 * only printed, so the numbers survive past one test run's console output.
 *
 * No charting library exists in this project's pom.xml, and Day 4 does not
 * need one -- SVG is plain text, and a small hand-built line chart is a few
 * dozen lines, not a dependency worth adding for one deliverable.
 */
final class WindowSweepReport {

    private WindowSweepReport() {
    }

    record Row(int windowSeconds, long seed, NetworkFaultType faultType,
            int truePositives, int falsePositives, int falseNegatives, double precision, double recall) {
    }

    static void writeCsv(Path path, List<Row> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("window_seconds,seed,fault_type,tp,fp,fn,precision,recall\n");
        for (Row row : rows) {
            sb.append(row.windowSeconds()).append(',')
                    .append(row.seed()).append(',')
                    .append(row.faultType()).append(',')
                    .append(row.truePositives()).append(',')
                    .append(row.falsePositives()).append(',')
                    .append(row.falseNegatives()).append(',')
                    .append(fmt(row.precision())).append(',')
                    .append(fmt(row.recall())).append('\n');
        }
        write(path, sb.toString());
    }

    /**
     * A single-series-per-metric line chart: mean overall precision and mean
     * overall recall, one point per window, x-axis log-scaled (the sweep
     * spans 1s to 60m -- four orders of magnitude, unreadable linearly).
     */
    static void writeChart(Path path, List<Integer> windows, List<Double> meanPrecision, List<Double> meanRecall) {
        int width = 720;
        int height = 420;
        int marginLeft = 60;
        int marginRight = 30;
        int marginTop = 30;
        int marginBottom = 60;
        int plotWidth = width - marginLeft - marginRight;
        int plotHeight = height - marginTop - marginBottom;

        double logMin = Math.log10(windows.get(0));
        double logMax = Math.log10(windows.get(windows.size() - 1));

        StringBuilder svg = new StringBuilder();
        svg.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(width)
                .append("\" height=\"").append(height).append("\" viewBox=\"0 0 ").append(width)
                .append(' ').append(height).append("\">\n");
        svg.append("<rect width=\"100%\" height=\"100%\" fill=\"white\"/>\n");

        // Axes
        svg.append(String.format(Locale.ROOT,
                "<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"black\"/>\n",
                marginLeft, marginTop + plotHeight, marginLeft + plotWidth, marginTop + plotHeight));
        svg.append(String.format(Locale.ROOT,
                "<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"black\"/>\n",
                marginLeft, marginTop, marginLeft, marginTop + plotHeight));

        // Y axis ticks at 0, 0.5, 1.0
        for (double frac : new double[] {0.0, 0.5, 1.0}) {
            int y = marginTop + (int) ((1 - frac) * plotHeight);
            svg.append(String.format(Locale.ROOT,
                    "<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"lightgray\"/>\n",
                    marginLeft, y, marginLeft + plotWidth, y));
            svg.append(String.format(Locale.ROOT,
                    "<text x=\"%d\" y=\"%d\" font-size=\"11\" text-anchor=\"end\">%.1f</text>\n",
                    marginLeft - 6, y + 4, frac));
        }

        // X axis labels, one per window
        for (int window : windows) {
            int x = marginLeft + xFor(window, logMin, logMax, plotWidth);
            svg.append(String.format(Locale.ROOT,
                    "<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke=\"lightgray\"/>\n",
                    x, marginTop, x, marginTop + plotHeight));
            svg.append(String.format(Locale.ROOT,
                    "<text x=\"%d\" y=\"%d\" font-size=\"10\" text-anchor=\"end\" "
                            + "transform=\"rotate(-40 %d %d)\">%s</text>\n",
                    x, marginTop + plotHeight + 16, x, marginTop + plotHeight + 16, humanWindow(window)));
        }

        svg.append(polyline(windows, meanPrecision, logMin, logMax, marginLeft, marginTop, plotWidth, plotHeight,
                "steelblue"));
        svg.append(polyline(windows, meanRecall, logMin, logMax, marginLeft, marginTop, plotWidth, plotHeight,
                "darkorange"));

        svg.append(String.format(Locale.ROOT,
                "<rect x=\"%d\" y=\"%d\" width=\"10\" height=\"10\" fill=\"steelblue\"/>"
                        + "<text x=\"%d\" y=\"%d\" font-size=\"12\">mean overall precision</text>\n",
                marginLeft + 10, marginTop, marginLeft + 24, marginTop + 9));
        svg.append(String.format(Locale.ROOT,
                "<rect x=\"%d\" y=\"%d\" width=\"10\" height=\"10\" fill=\"darkorange\"/>"
                        + "<text x=\"%d\" y=\"%d\" font-size=\"12\">mean overall recall</text>\n",
                marginLeft + 10, marginTop + 16, marginLeft + 24, marginTop + 25));

        svg.append("</svg>\n");
        write(path, svg.toString());
    }

    private static String polyline(List<Integer> windows, List<Double> values, double logMin, double logMax,
            int marginLeft, int marginTop, int plotWidth, int plotHeight, String color) {
        StringBuilder points = new StringBuilder();
        for (int i = 0; i < windows.size(); i++) {
            int x = marginLeft + xFor(windows.get(i), logMin, logMax, plotWidth);
            double v = values.get(i);
            int y = marginTop + (int) ((1 - (Double.isNaN(v) ? 0 : v)) * plotHeight);
            points.append(x).append(',').append(y).append(' ');
        }
        return String.format(Locale.ROOT,
                "<polyline points=\"%s\" fill=\"none\" stroke=\"%s\" stroke-width=\"2\"/>\n",
                points.toString().trim(), color);
    }

    private static int xFor(int windowSeconds, double logMin, double logMax, int plotWidth) {
        double log = Math.log10(windowSeconds);
        double frac = logMax == logMin ? 0 : (log - logMin) / (logMax - logMin);
        return (int) (frac * plotWidth);
    }

    private static String humanWindow(int seconds) {
        if (seconds < 60) {
            return seconds + "s";
        }
        if (seconds < 3600) {
            return (seconds / 60) + "m";
        }
        return (seconds / 3600) + "h";
    }

    private static String fmt(double value) {
        return Double.isNaN(value) ? "" : String.format(Locale.ROOT, "%.4f", value);
    }

    private static void write(Path path, String content) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
