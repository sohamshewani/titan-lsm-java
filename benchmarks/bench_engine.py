package com.titan.lsm.benchmark;

import com.titan.lsm.MemTable;
import com.titan.lsm.SSTable;
import com.titan.lsm.WriteBatch;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TitanLSMBenchmark {

    private static final int WARMUP_ITERATIONS = 50_000;
    private static final int MEASUREMENT_ITERATIONS = 200_000;
    private static final int KEY_SPACE_SIZE = 1_000_000;
    
    private static final int KEY_SIZE = 16;
    private static final int VALUE_SIZE = 100;

    private static final double WRITE_RATIO = 0.5; // 50% Writes, 50% Reads

    private static byte[] generateKey(int id) {
        String keyStr = String.format("key-%012d", id);
        return keyStr.getBytes();
    }

    private static byte[] generateValue(int id) {
        String valStr = String.format("val-%096d", id);
        return valStr.getBytes();
    }

    public static void main(String[] args) throws Exception {
        Path tempDir = Files.createTempDirectory("titan-lsm-bench-");
        
        try {
            // Initialize LSM components
            MemTable memTable = new MemTable();
            
            System.out.println("Preparing benchmark environment...");
            Random random = new Random(42);

            // Pre-generate keys and values to eliminate allocation overhead during measurement
            byte[][] testKeys = new byte[MEASUREMENT_ITERATIONS][];
            byte[][] testValues = new byte[MEASUREMENT_ITERATIONS][];
            boolean[] isWrite = new boolean[MEASUREMENT_ITERATIONS];

            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                int keyId = random.nextInt(KEY_SPACE_SIZE);
                testKeys[i] = generateKey(keyId);
                testValues[i] = generateValue(keyId);
                isWrite[i] = random.nextDouble() < WRITE_RATIO;
            }

            // --- WARMUP PHASE ---
            System.out.println("Running warmup phase (" + WARMUP_ITERATIONS + " ops)...");
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                int keyId = random.nextInt(KEY_SPACE_SIZE);
                byte[] k = generateKey(keyId);
                byte[] v = generateValue(keyId);
                if (i % 2 == 0) {
                    memTable.put(k, v);
                } else {
                    memTable.get(k);
                }
            }

            // --- MEASUREMENT PHASE ---
            System.out.println("Running measurement phase (" + MEASUREMENT_ITERATIONS + " ops)...");
            long[] latencies = new long[MEASUREMENT_ITERATIONS];

            long startTime = System.nanoTime();

            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                long opStart = System.nanoTime();

                if (isWrite[i]) {
                    memTable.put(testKeys[i], testValues[i]);
                } else {
                    memTable.get(testKeys[i]);
                }

                long opEnd = System.nanoTime();
                latencies[i] = opEnd - opStart;
            }

            long endTime = System.nanoTime();
            long totalDurationNs = endTime - startTime;

            // --- CALCULATE METRICS ---
            double totalSeconds = totalDurationNs / 1_000_000_000.0;
            double opsPerSec = MEASUREMENT_ITERATIONS / totalSeconds;

            // Sort latencies for percentiles
            long[] sortedLatencies = new long[MEASUREMENT_ITERATIONS];
            System.arraycopy(latencies, 0, sortedLatencies, 0, MEASUREMENT_ITERATIONS);
            java.util.Arrays.sort(sortedLatencies);

            double p50 = sortedLatencies[(int) (MEASUREMENT_ITERATIONS * 0.50)] / 1_000.0; // in microseconds
            double p90 = sortedLatencies[(int) (MEASUREMENT_ITERATIONS * 0.90)] / 1_000.0;
            double p99 = sortedLatencies[(int) (MEASUREMENT_ITERATIONS * 0.99)] / 1_000.0;
            double p999 = sortedLatencies[(int) (MEASUREMENT_ITERATIONS * 0.999)] / 1_000.0;
            double max = sortedLatencies[MEASUREMENT_ITERATIONS - 1] / 1_000.0;

            // --- OUTPUT RESULTS ---
            System.out.println("\n==================================================");
            System.out.println("       TITAN-LSM-JAVA MICROBENCHMARK RESULTS      ");
            System.out.println("==================================================");
            System.out.printf("%-25s | %-20s%n", metricHeader("Metric"), metricHeader("Value"));
            System.out.println("--------------------------------------------------");
            System.out.printf("%-25s | %-20.2f%n", "Throughput (ops/sec)", opsPerSec);
            System.out.printf("%-25s | %-20.2f%n", "Total Operations", (double) MEASUREMENT_ITERATIONS);
            System.out.printf("%-25s | %-20.2f%n", "Duration (seconds)", totalSeconds);
            System.out.printf("%-25s | %-20.2f%n", "p50 Latency (µs)", p50);
            System.out.printf("%-25s | %-20.2f%n", "p90 Latency (µs)", p90);
            System.out.printf("%-25s | %-20.2f%n", "p99 Latency (µs)", p99);
            System.out.printf("%-25s | %-20.2f%n", "p99.9 Latency (µs)", p999);
            System.out.printf("%-25s | %-20.2f%n", "Max Latency (µs)", max);
            System.out.println("==================================================");

        } finally {
            // Cleanup temp files recursively
            deleteDirectory(tempDir.toFile());
        }
    }

    private static String metricHeader(String s) {
        return s;
    }

    private static void deleteDirectory(File f) {
        File[] allFiles = f.listFiles();
        if (allFiles != null) {
            for (File file : allFiles) {
                deleteDirectory(file);
            }
        }
        f.delete();
    }
}