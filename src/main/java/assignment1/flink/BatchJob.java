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

package assignment1.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> data = env.readTextFile("file:///Users/patrickmao/IdeaProjects/flink/assignment1/cab-flink.txt");

        // map data
        DataSet<Tuple8<Integer, String, String, String, Boolean, String, String, Integer>> records = data.map(new MapFunction<String, Tuple8<Integer, String, String, String, Boolean, String, String, Integer>>() {
            @Override
            public Tuple8<Integer, String, String, String, Boolean, String, String, Integer> map(String record) throws Exception {
                String[] fields = record.split(",");
                Integer id = Integer.parseInt(fields[0].split("_")[1]);
                Boolean isOngoing = "yes".equals(fields[4]);
                Integer passengerCount = fields[7].equals("\'null\'") ? 0 : Integer.parseInt(fields[7]);
                return new Tuple8<>(id, fields[1], fields[2], fields[3], isOngoing, fields[5], fields[6], passengerCount);
            }
        });

        // find most popular destination
        DataSet<Tuple2<String, Integer>> destinationRanking = records
                .filter(new FilterFunction<Tuple8<Integer, String, String, String, Boolean, String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, String, String, String, Boolean, String, String, Integer> record) {
                        return !record.f6.equals("\'null\'");
                    }
                })
                .map(new MapFunction<Tuple8<Integer, String, String, String, Boolean, String, String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple8<Integer, String, String, String, Boolean, String, String, Integer> record) {
                        return new Tuple2<>(record.f6, record.f7);
                    }
                })
                .groupBy(0)
                .sum(1)
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1);
        ;
        destinationRanking.writeAsCsv("file:///Users/patrickmao/IdeaProjects/flink/assignment1/out/destinationRankings.csv", "\n", ",", FileSystem.WriteMode.OVERWRITE);

        // execute program
        env.execute("Most Popular Destination");
    }
}
