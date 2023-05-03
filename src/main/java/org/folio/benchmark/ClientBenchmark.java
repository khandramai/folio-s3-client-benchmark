package org.folio.benchmark;

import org.folio.client.RemoteFileSystemClient;
import org.folio.s3.client.S3ClientProperties;
import org.folio.server.MinioServer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.folio.benchmark.ClientBenchmark.ExecutionPlan.NUM_OF_ITERATIONS;

public class ClientBenchmark {

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        public static final int NUM_OF_ITERATIONS = 10;

        @Param({"1000", "10000", "100000", "1000000"})
        public int size;
        @Param({"100", "1000", "10000", "100000"})
        public int iterations;
        public RemoteFileSystemClient client;
        public String id;
        public String data;

        public MinioServer server;

        @Setup(Level.Trial)
        public void globalSetUp() throws IOException {
            System.out.println("Setup global");

            Thread printingHook = new Thread(() -> server.stop());
            Runtime.getRuntime().addShutdownHook(printingHook);

            var credentials = new MinioServer.CredentialsProvider("minioadmin", "minioadmin");
            server = new MinioServer(credentials);
            server.start();

            var s3ClientProperties = S3ClientProperties.builder()
                    .accessKey(credentials.getAccessKey())
                    .secretKey(credentials.getAccessKey())
                    .endpoint(server.getHostAddress())
                    .region("us-west")
                    .bucket("benchmark-bucket")
                    .awsSdk(false)
                    .build();

            client = new RemoteFileSystemClient(s3ClientProperties);
            try (var is = new FileInputStream("src/main/resources/complete_user.json")) {
                data = new String(is.readAllBytes()) + "\n";
            }
        }

        @Setup(Level.Trial)
        public void globalTearDown() {
            server.stop();
        }

        @Setup(Level.Invocation)
        public void setUp() {
            id = UUID.randomUUID().toString();
        }
    }

    @Fork(value = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 5)
    @Measurement(iterations = NUM_OF_ITERATIONS, time = 5)
    public void writeWithFileBuffer(ExecutionPlan plan) {
        try (var writer = plan.client.fileBufferedWriter("/file-buffer/" + plan.id)) {
            for (int i=0; i < plan.iterations; i++) {
                writer.write(plan.data);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Fork(value = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 5)
    @Measurement(iterations = NUM_OF_ITERATIONS, time = 5)
    public void writeWithOptimizedFileBuffer(ExecutionPlan plan) {
        try (var writer = plan.client.optimizedFileWriter("/file-optimized-buffer/" + plan.id, plan.size)) {
            for (int i=0; i < plan.iterations; i++) {
                writer.write(plan.data);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Fork(value = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 5, time = 5)
    @Measurement(iterations = NUM_OF_ITERATIONS, time = 5)
    public void writeWithMemoryBuffer(ExecutionPlan plan) {
        try (var writer = plan.client.bufferedAppendWriter("/mem-buffer/" + plan.size + "/" + plan.id, plan.size)) {
            for (int i = 0; i < plan.iterations; i++) {
                writer.write(plan.data);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}