package com.matthewrathbone.helpers;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import java.io.File;
import java.io.StringWriter;
import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.io.IOUtils;

public class MapReduceTester {
  private final File base = new File("target/test");
  private final Path baseDir = new Path("file://" + base.getAbsolutePath());
  private JobConf conf = new JobConf();
  private FileSystem fileSystem;
  protected int datasetNum = 0;

  public static class JobArgs {
    public Configuration conf;
    public FileSystem fs;
    public JobArgs(Configuration conf, FileSystem fs) {
      this.conf = conf;
      this.fs = fs;
    }
  }

  public static abstract class TestJob {
    public abstract void run(JobArgs args);
  }

  public MapReduceTester() throws Exception {
    String mrDir = "mapred/local";
    String hdLog = "logs";
    
    conf.set("mapred.local.dir", new File(base, mrDir).getAbsolutePath());
    conf.set("mapred.job.tracker.handle.count", "2");
    conf.set("tasktracker.http.threads", "2");
    conf.set("mapred.map.max.attempts", "2");
    conf.set("mapred.reduce.max.attempts", "2");
    conf.set("jobclient.completion.poll.interval", "30");
    System.getProperties().setProperty("hadoop.log.dir", new File(base, hdLog).getAbsolutePath());
    this.fileSystem = FileSystem.get(baseDir.toUri(), conf);
    assert(fileSystem.mkdirs(new Path(baseDir, mrDir)));
    assert(fileSystem.mkdirs(new Path(baseDir, hdLog)));
  }

  public Path getDirectory() throws Exception {
    Path destination = new Path(baseDir, String.valueOf(datasetNum));
    fileSystem.delete(destination, true);
    fileSystem.mkdirs(destination);
    datasetNum += 1;
    return destination;
  }

  public List<String> collectText(Path location) throws Exception {
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    FileStatus[] items = fileSystem.listStatus(location);
    if (items == null) return new ArrayList<String>();
    List<String> results = new ArrayList<String>();
    for(FileStatus item: items) {
      CompressionCodec codec = factory.getCodec(item.getPath());
      InputStream stream = null;
      if (codec != null) stream = codec.createInputStream(fileSystem.open(item.getPath()));
      else stream = fileSystem.open(item.getPath());

      StringWriter writer = new StringWriter();
      IOUtils.copy(stream, writer, "UTF-8");
      String raw = writer.toString();
      for(String str: raw.split("\n")) {
        results.add(str);
      }
    }
    return results;
  }

  public void test(TestJob job) {
    JobArgs args = new JobArgs(conf, fileSystem);
    job.run(args);
  }


}