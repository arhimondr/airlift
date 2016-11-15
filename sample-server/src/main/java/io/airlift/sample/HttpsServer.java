package io.airlift.sample;

import com.google.common.collect.ImmutableSet;
import io.airlift.event.client.NullEventClient;
import io.airlift.http.server.HttpServer;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.HttpServerProvider;
import io.airlift.http.server.RequestStats;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class HttpsServer
{
    private static final int HTTP_PORT = 8910;
    private static final int HTTPS_PORT = 7878;
    private static final int CONTENT_LENGTH = 5 * 1024 * 1024;
    private static final byte[] CONTENT = new byte[CONTENT_LENGTH];

    static {
        new Random().nextBytes(CONTENT);
    }

    public static void main(String[] args)
            throws Exception
    {
        HttpServerConfig serverConfig = new HttpServerConfig();
        serverConfig.setSecureRandomAlgorithm(args[0]);
        serverConfig.setHttpEnabled(true);
        serverConfig.setHttpsEnabled(true);
        serverConfig.setHttpPort(HTTP_PORT);
        serverConfig.setHttpsPort(HTTPS_PORT);
        serverConfig.setKeystorePath("/etc/presto/labs_teradata_com.jks");
        serverConfig.setKeystorePassword("123456");

        NodeConfig nodeConfig = new NodeConfig();
        nodeConfig.setEnvironment("test");
        NodeInfo nodeInfo = new NodeInfo(nodeConfig);

        HttpServerProvider provider = new HttpServerProvider(
                new HttpServerInfo(serverConfig, nodeInfo),
                nodeInfo,
                serverConfig,
                new Servlet(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                new RequestStats(),
                new NullEventClient());

        HttpServer httpServer = provider.get();
        httpServer.start();
        System.out.println("Server started on port: " + HTTPS_PORT);
        TimeUnit.MINUTES.sleep(100);
    }

    private static class Servlet
            extends HttpServlet
    {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException
        {
            resp.setContentLength(CONTENT_LENGTH);
            try (OutputStream outputStream = resp.getOutputStream()) {
                outputStream.write(CONTENT);
            }
        }
    }
}
