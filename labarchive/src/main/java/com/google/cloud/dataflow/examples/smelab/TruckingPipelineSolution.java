/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.smelab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * SME Training Basic CodeLab
 */
@SuppressWarnings("serial")
public class TruckingPipelineSolution {
  private static final Logger LOG =
      LoggerFactory.getLogger(TruckingPipelineSolution.class);

  public static void main(String[] args) {
    // Define a pipeline object.
    Pipeline p =
        Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());

    // Before running this in the Cloud, you MUST create a storage bucket:
    // https://cloud.google.com/storage/docs/creating-buckets
    // This code assumes you uploaded the sample logs folder to your
    // staging bucket.
    String filePath = p.getOptions().getTempLocation();
    final boolean running_in_cloud;
    if (p.getOptions().getRunner().getSimpleName()
         .equals("DirectPipelineRunner")) {
      running_in_cloud = false;
    } else {
      running_in_cloud = true;
    }

    // Read in the log files using the TextIO source class.
    // See: https://cloud.google.com/dataflow/model/text-io
    PCollection<String> lines;
    if (running_in_cloud) {
      // Running in the Cloud. Use data in Cloud Storage.
      // TODO: <<optional>> Modify the location to read all log files.
      // See: https://cloud.google.com/dataflow/model/pipeline-io#using-reads
      lines =
          p.apply(TextIO.Read.from(filePath + "/logs/package_log_*.txt"));
    } else {
      // Running locally. Use sample log data.
      lines = p.apply(Create.of(PackageActivityInfo.MINI_LOG));
    }

    PCollection<PackageActivityInfo> packages =
         // ParDo is a transform that will call processElement of the
         // encapsulated DoFn on each element of the input PCollection.
         // PackageActivityInfo.ParseLine() is a DoFn class that parses each
         // input log string into a PackageActivityInfo object. The DoFn
         // definition can be seen in PackageActivityInfo.java.
        lines.apply(ParDo.of(new PackageActivityInfo.ParseLine()))
             // This transform uses the toString method defined on
         // PackageActivityInfo to print back out the objects.
         // This is used to verify the reading and parsing of
         // the test log files and should not be executed when
         // running in the cloud.
         .apply(ParDo.of(new DoFn<PackageActivityInfo, PackageActivityInfo>() {
           @Override
           public void processElement(ProcessContext c) {
             if (!running_in_cloud) {
               // c.element is a single element from the input PCollection.
               LOG.info(c.element().toString());
             }
             // Each c.output call adds one element to the output PCollection
             // of the ParDo.
             c.output(c.element());
           }
         }));

    // In order to apply keyed operations, each PackageActivityInfo must be
    // associated with a key. Use WithKeys to extract the location field as
    // the key for each PacakgeActivityInfo. See:
    // https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/WithKeys
    PCollection<KV<String, PackageActivityInfo>> keyedPackages =
        packages.apply(// TODO #1 WithKeys to extract location as key
             WithKeys.of(new SerializableFunction<PackageActivityInfo, String>() {
               public String apply(PackageActivityInfo s) { return s.getLocation(); }
               }));

    // Keyed Operations:
    // 1. Count the number of packages per location. Use Count. For an example,
    // See: https://cloud.google.com/dataflow/examples/wordcount-example
    PCollection<KV<String, Long>> counts =
        keyedPackages.apply( // TODO #2 Count perKey
                            Count.<String,PackageActivityInfo> perKey());

    // Apply a ParDo of an anonymous DoFn which transforms the
    // input KV<String, Long> to an output log String.
    PCollection<String> formattedCountStrings = counts.apply(ParDo.of(
         new DoFn<KV<String, Long>, String>() {
           @Override
           public void processElement(ProcessContext c) {
                              c.output(c.element().getKey() + ": "
                                       + c.element().getValue());
                              LOG.info("Processing key: " + c.element()
                                                             .getKey());
           }
         }));
         
    if (running_in_cloud) {
      // Use TextIO.Write to write the output string to a file.
      formattedCountStrings.apply(TextIO.Write.to(filePath + "/output/output.txt"));
    } else {
      formattedCountStrings.apply(ParDo.of(new DoFn<String, Void>() {
                                    @Override
                                    public void processElement(ProcessContext c) {
                                        LOG.info(c.element());
                                    }}));
    }

    p.run();
  }
}
