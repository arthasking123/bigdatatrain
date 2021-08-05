/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.geek;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class GeekTextInputFormat implements
    InputFormat<LongWritable, BytesWritable>, JobConfigurable {

  public static class GeekLineRecordReader implements
      RecordReader<LongWritable, BytesWritable>, JobConfigurable {

    LineRecordReader reader;
    Text text;

    public GeekLineRecordReader(LineRecordReader reader) {
      this.reader = reader;
      text = reader.createValue();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public LongWritable createKey() {
      return reader.createKey();
    }

    @Override
    public BytesWritable createValue() {
      return new BytesWritable();
    }

    @Override
    public long getPos() throws IOException {
      return reader.getPos();
    }

    @Override
    public float getProgress() throws IOException {
      return reader.getProgress();
    }

    @Override
    public boolean next(LongWritable key, BytesWritable value) throws IOException {
      while (reader.next(key, text)) {
        String replacedString = text.toString().replaceAll(" ge{2,256}k", "");
        value.set(replacedString.getBytes(), 0, replacedString.length());
        return true;
      }
      // no more data
      return false;
    }

    @Override
    public void configure(JobConf job) {

    }

  }

  TextInputFormat format;
  JobConf job;

  public GeekTextInputFormat() {
    format = new TextInputFormat();
  }

  @Override
  public void configure(JobConf job) {
    this.job = job;
    format.configure(job);
  }

  public RecordReader<LongWritable, BytesWritable> getRecordReader(
      InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(genericSplit.toString());
    GeekLineRecordReader reader = new GeekLineRecordReader(
        new LineRecordReader(job, (FileSplit) genericSplit));
    reader.configure(job);
    return reader;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return format.getSplits(job, numSplits);
  }

}
