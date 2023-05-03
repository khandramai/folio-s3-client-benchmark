package org.folio.server;


import lombok.AllArgsConstructor;
import lombok.Data;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.time.Duration;

public class MinioServer extends GenericContainer<MinioServer> {

    private static final int DEFAULT_PORT = 9000;
    private static final String DEFAULT_IMAGE = "minio/minio";
    private static final String DEFAULT_TAG = "edge";
    public static final String MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY";
    private static final String MINIO_SECRET_KEY = "MINIO_SECRET_KEY";
    private static final String DEFAULT_STORAGE_DIRECTORY = "/folio-s3-client";
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";

    public MinioServer(CredentialsProvider credentials) {
        this(DEFAULT_IMAGE + ":" + DEFAULT_TAG, credentials);
    }

    public MinioServer(String image, CredentialsProvider credentials) {
        super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);
        withNetworkAliases("minio-" + Base58.randomString(6));
        addExposedPort(DEFAULT_PORT);
        if (credentials != null) {
            withEnv(MINIO_ACCESS_KEY, credentials.getAccessKey());
            withEnv(MINIO_SECRET_KEY, credentials.getSecretKey());
        }
        withCommand("server", DEFAULT_STORAGE_DIRECTORY);
        setWaitStrategy(new HttpWaitStrategy()
                .forPort(DEFAULT_PORT)
                .forPath(HEALTH_ENDPOINT)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    @Override
    public void start() {
        super.start();
        System.out.println("Minio server started on path: " + getHostAddress());
    }

    @Override
    public void stop() {
        super.stop();
        System.out.println("Minio server stopped");
    }

    public String getHostAddress() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(DEFAULT_PORT);
    }

    @Data
    @AllArgsConstructor
    public static class CredentialsProvider {
        private String accessKey;
        private String secretKey;
    }
}
