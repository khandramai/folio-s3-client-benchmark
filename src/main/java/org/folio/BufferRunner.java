package org.folio;

import org.folio.client.RemoteFileSystemClient;
import org.folio.s3.client.S3ClientProperties;
import org.folio.server.MinioServer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;


public class BufferRunner {

    public static void main(String[] args) throws IOException {

        var credentials = new MinioServer.CredentialsProvider("minioadmin", "minioadmin");
        try (var server = new MinioServer(credentials);
             var dis = new FileInputStream("src/main/resources/complete_user.json")) {

            server.start();

            var s3ClientProperties = S3ClientProperties.builder()
                    .accessKey(credentials.getAccessKey())
                    .secretKey(credentials.getAccessKey())
                    .endpoint(server.getHostAddress())
                    .region("us-west")
                    .bucket("benchmark-bucket")
                    .awsSdk(false)
                    .build();

            var client = new RemoteFileSystemClient(s3ClientProperties);

            var data = new String(dis.readAllBytes()) + "\n";

            var id = UUID.randomUUID();
            var size = 5;

            try (var writer = client.optimizedFileWriter("/mem-buffer/" + size + "/" + id, size)) {
                for (int i = 0; i < 10; i++) {
                    writer.write(data);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
