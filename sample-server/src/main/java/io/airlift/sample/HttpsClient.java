package io.airlift.sample;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.concurrent.TimeUnit.MINUTES;

public class HttpsClient
{
    public static void main(String[] args)
    {
        String address = args[0];
        Duration duration = Duration.valueOf(args[1]);
        int concurrency = Integer.parseInt(args[2]);

        HttpClientConfig config = new HttpClientConfig();
        config.setKeyStorePath("/etc/presto/labs_teradata_com.jks");
        config.setKeyStorePassword("123456");
        HttpClient httpClient = new JettyHttpClient(config);

        long deadline = System.currentTimeMillis() + duration.toMillis();

        Request request = prepareGet().setUri(URI.create(address)).build();

        List<Callable<Long>> tasks = IntStream.range(0, concurrency)
                .mapToObj(i -> new FetchDataTask(httpClient, request, deadline))
                .collect(Collectors.toList());

        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            List<Long> results = executor.invokeAll(tasks)
                    .stream()
                    .map(Futures::getUnchecked)
                    .collect(Collectors.toList());

            long totalKb = 0;
            long seconds = duration.toMillis() / 1000;
            for (int i = 0; i < results.size(); i++) {
                long kb = results.get(i) / 1024;
                totalKb += kb;
                long kbPerSecond = kb / seconds;
                System.out.printf("[Thread #%d] Throughput: %dkb/s. Transferred: %dkb\n", i, kbPerSecond, kb);
            }

            long kbPerSecond = totalKb / seconds;
            System.out.printf("[Total] Throughput: %dkb/s. Transferred: %dkb", kbPerSecond, totalKb);
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

    private static class FetchDataTask
            implements Callable<Long>
    {
        private final HttpClient httpClient;
        private final Request request;
        private final long deadline;

        private FetchDataTask(HttpClient httpClient, Request request, long deadline)
        {
            this.httpClient = httpClient;
            this.request = request;
            this.deadline = deadline;
        }

        @Override
        public Long call()
                throws Exception
        {
            long bytesRead = 0;
            while (!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < deadline) {
                bytesRead += httpClient.execute(request, new ResponseHandler<Long, RuntimeException>()
                {
                    @Override
                    public Long handleException(Request request, Exception exception)
                            throws RuntimeException
                    {
                        throw Throwables.propagate(exception);
                    }

                    @Override
                    public Long handle(Request request, Response response)
                            throws RuntimeException
                    {
                        try (InputStream in = response.getInputStream()) {
                            return ByteStreams.copy(in, ByteStreams.nullOutputStream());
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
            }
            return bytesRead;
        }
    }
}
