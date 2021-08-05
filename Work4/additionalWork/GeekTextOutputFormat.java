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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.*;

public class GeekTextOutputFormat<K extends WritableComparable, V extends Writable>
    extends HiveIgnoreKeyTextOutputFormat<K, V> {

  public static class GeekRecordWriter implements RecordWriter,
      JobConfigurable {

    RecordWriter writer;
    BytesWritable bytesWritable;

    public GeekRecordWriter(RecordWriter writer) {
      this.writer = writer;
      bytesWritable = new BytesWritable();
    }

    private int getRandomNum(int remainWords) {
      int max = Math.min(256, remainWords);
      int min = 2;
      if (max < min) {
        return -1;
      }
      return (int) (Math.random() * (max - min) + min);
    }

    private String mergeString(String[] originalWordsArray, List<Integer> insertPosArray) {
      String result = "";
      int startOffset = 0;
      for (Integer insertOffset : insertPosArray) {
        result += String.join(" ", Arrays.copyOfRange(originalWordsArray, startOffset, insertOffset));
        result += " g" + String.join("", Collections.nCopies(insertOffset - startOffset, "e")) + "k ";
        startOffset = insertOffset;
      }
      if(startOffset < originalWordsArray.length){
        result += String.join(" ", Arrays.copyOfRange(originalWordsArray, startOffset, originalWordsArray.length ));
      }
      return result;
    }

    @Override
    public void write(Writable w) throws IOException {
      assert w instanceof Text || (w instanceof BytesWritable);
      String wString;
      wString = w.toString();

      String[] words = wString.split(" ");
      int remainProcessingWords = words.length;
      List<Integer> insertPosArray = new ArrayList<>();
      while(remainProcessingWords != 0) {
        int randomNum = getRandomNum(remainProcessingWords);
        if(-1 == randomNum){
          break;
        }
        insertPosArray.add(words.length - remainProcessingWords + randomNum);
        remainProcessingWords -= randomNum;
      }
      byte[] output = mergeString(words, insertPosArray).getBytes();
      bytesWritable.set(output, 0, output.length);

      writer.write(bytesWritable);
    }

    @Override
    public void close(boolean abort) throws IOException {
      writer.close(abort);
    }

    @Override
    public void configure(JobConf job) {
    }
  }

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {

    GeekRecordWriter writer = new GeekRecordWriter(super
        .getHiveRecordWriter(jc, finalOutPath, BytesWritable.class,
        isCompressed, tableProperties, progress));
    writer.configure(jc);
    return writer;
  }

}
