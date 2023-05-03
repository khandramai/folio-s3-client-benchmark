package org.folio.client;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.client.MinioS3Client;
import org.folio.s3.client.S3ClientProperties;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;


public class RemoteFileSystemClient {

  public final FolioS3Client remoteFolioS3Client;

  public RemoteFileSystemClient(S3ClientProperties properties) {
    remoteFolioS3Client = new MinioS3Client(properties);
  }

  public String put(InputStream newFile, String fileNameToBeUpdated) {
    return remoteFolioS3Client.write(fileNameToBeUpdated, newFile);
  }

  public String append(InputStream content, String fileNameToAppend) {
    return remoteFolioS3Client.append(fileNameToAppend, content);
  }

  public int getNumOfLines(String file) {
    return (int) new BufferedReader(new InputStreamReader(get(file))).lines().count();
  }

  // Code below should be moved to folio-s3-client
  public InputStream get(String fileName) {
    return remoteFolioS3Client.read(fileName);
  }

  public void remove(String filename) {
    remoteFolioS3Client.remove(filename);
  }

  public Writer fileBufferedWriter(String path) {
    return new FileBufferWriter(path);
  }

  public Writer optimizedFileWriter(String path, int size) {
    return new OptimizedFileWriter(path, size);
  }

  public Writer directAppendWriter(String path) {
    return new DirectAppendWriter(path);
  }

  public Writer bufferedAppendWriter(String path, int size) {
    return new BufferedAppendWriter(path, size);
  }

  public class OptimizedFileWriter extends StringWriter {

    private final File tmp;
    private final String path;
    private final BufferedWriter writer;


    public OptimizedFileWriter(String path, int size) {
      try {
        this.path = path;
        tmp = Files.createTempFile(FilenameUtils.getName(path), FilenameUtils.getExtension(path)).toFile();
        writer = new BufferedWriter(new FileWriter(tmp), size);
      } catch (IOException e) {
        throw new RuntimeException("Files buffer cannot be created due to error: ", e);
      }
    }

    @Override
    public void write(String data) {
      if (StringUtils.isNotEmpty(data)) {
        try {
          writer.append(data);
        } catch (IOException e) {
          try {
            Files.deleteIfExists(tmp.toPath());
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      } else {
        try {
          Files.deleteIfExists(tmp.toPath());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void close() {
      try {
        if (tmp.exists()) {
          put(FileUtils.openInputStream(tmp), path);
        }
      } catch (Exception e) {
        // Just skip and wait file deletion
      } finally {
        try {
          Files.deleteIfExists(tmp.toPath());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public class FileBufferWriter extends StringWriter {

    private final Path tmp;
    private final String path;
    public FileBufferWriter(String path) {
      try {
        this.path = path;
        tmp = Files.createTempFile(FilenameUtils.getName(path), FilenameUtils.getExtension(path));
      } catch (IOException e) {
        throw new RuntimeException("Files buffer cannot be created due to error: ", e);
      }
    }

    @Override
    public void write(String data) {
      if (StringUtils.isNotEmpty(data)) {
        try {
          FileUtils.write(tmp.toFile(), data, Charset.defaultCharset(), true);
        } catch (IOException e) {
          FileUtils.deleteQuietly(tmp.toFile());
        }
      } else {
        FileUtils.deleteQuietly(tmp.toFile());
      }
    }

    @Override
    public void close() {
      try {
        if (tmp.toFile().exists()) {
          put(FileUtils.openInputStream(tmp.toFile()), path);
        }
      } catch (Exception e) {
        // Just skip and wait file deletion
      } finally {
        FileUtils.deleteQuietly(tmp.toFile());
      }
    }
  }

  public class DirectAppendWriter extends StringWriter {

    private final String path;

    public DirectAppendWriter(String path) {
      this.path = path;
    }

    @Override
    public void write(String data) {
      RemoteFileSystemClient.this.append(new ByteArrayInputStream(data.getBytes()), path);
    }
  }

  public class BufferedAppendWriter extends StringWriter {

    private final String path;
    private StringBuilder buffer;
    private final int size;

    public BufferedAppendWriter(String path, int size) {
      this.size = size;
      this.path = path;
      buffer = new StringBuilder(size);//ByteBuffer.allocate(size);
    }

    @Override
    public synchronized void write(String b) {

      if (buffer.length() + b.length() < size) {
        buffer.append(b);
      } else {
        try (var input = IOUtils.toInputStream(buffer, Charset.defaultCharset())) {
          RemoteFileSystemClient.this.append(input, path);
        } catch (IOException e) {
          // Just skip writing and clean buffer
        } finally {
          buffer = new StringBuilder(size);
          buffer.append(b);
        }
      }
    }


    @Override
    public void close() {
      try(var input = IOUtils.toInputStream(buffer, Charset.defaultCharset())) {
        RemoteFileSystemClient.this.append(input, path);
      } catch (IOException e) {
        // Just skip writing and clean buffer
      } finally {
        buffer = null;
      }
    }
  }
}