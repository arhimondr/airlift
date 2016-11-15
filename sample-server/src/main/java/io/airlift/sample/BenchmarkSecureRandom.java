package io.airlift.sample;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import io.airlift.units.Duration;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BenchmarkSecureRandom
{
    private static final int BUFFER_SIZE = 16;


    public static void main(String[] args)
            throws InterruptedException
    {
        int durationSeconds = 30;
        SecureRandom nativePrng = getSecureRandom("NativePRNG");
        benchmark("NativePRNG:Singleton", () -> nativePrng, 1, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("NativePRNG:Singleton", () -> nativePrng, 4, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("NativePRNG:Singleton", () -> nativePrng, 24, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("NativePRNG:ThreadLocal", () -> getSecureRandom("NativePRNG"), 4, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("NativePRNG:ThreadLocal", () -> getSecureRandom("NativePRNG"), 24, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        SecureRandom sha1prng = getSecureRandom("SHA1PRNG");
        benchmark("SHA1PRNG:Singleton", () -> sha1prng, 1, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("SHA1PRNG:Singleton", () -> sha1prng, 4, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("SHA1PRNG:Singleton", () -> sha1prng, 24, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("SHA1PRNG:ThreadLocal", () -> getSecureRandom("SHA1PRNG"), 4, new Duration(durationSeconds, SECONDS));
        Thread.sleep(10_000);
        benchmark("SHA1PRNG:ThreadLocal", () -> getSecureRandom("SHA1PRNG"), 24, new Duration(durationSeconds, SECONDS));
    }

    private static SecureRandom getSecureRandom(String algorithm)
    {
        try {
            return SecureRandom.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void benchmark(String description, Supplier<SecureRandom> random, int concurrency, Duration duration)
    {
        System.out.print("\n\n");
        System.out.printf("[%s] Concurrency: %d, Duration: %s\n", description, concurrency, duration);
        long durationInMillis = duration.toMillis();
        long deadline = System.currentTimeMillis() + durationInMillis;
        List<BenchmarkTask> tasks = IntStream.range(0, concurrency)
                .mapToObj((i) -> new BenchmarkTask(random.get(), deadline))
                .collect(Collectors.toList());
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            List<Long> results = executor.invokeAll(tasks)
                    .stream()
                    .map(Futures::getUnchecked)
                    .collect(Collectors.toList());

            long totalKb = 0;
            long seconds = durationInMillis / 1000;
            for (int i = 0; i < results.size(); i++) {
                long kb = results.get(i) / 1024;
                totalKb += kb;
                long kbPerSecond = kb / seconds;
                System.out.printf("[Thread #%d] Throughput: %dkb/s. Generated: %dkb\n", i, kbPerSecond, kb);
            }

            long kbPerSecond = totalKb / seconds;
            System.out.printf("[Total] Throughput: %dkb/s. Generated: %dkb", kbPerSecond, totalKb);
        }
        catch (InterruptedException e) {
            // finish execution
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, MINUTES);
        }
        catch (InterruptedException e) {
            // finish execution
        }
    }

    private static class BenchmarkTask
            implements Callable<Long>
    {
        private final SecureRandom random;
        private final long deadline;
        private final byte[] buffer;

        private BenchmarkTask(SecureRandom secureRandom, long deadline)
        {
            this.random = secureRandom;
            this.deadline = deadline;
            this.buffer = new byte[BUFFER_SIZE];
        }

        @Override
        public Long call()
        {
            long bytesGenerated = 0;
            while (!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < deadline) {
                random.nextBytes(buffer);
                bytesGenerated += buffer.length;
            }
            return bytesGenerated;
        }
    }
}
