package com.isaac.flink.java.train02;

// use java api to develop Flink Batch Processing program

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.Int;

public class BatchWCJavaApp {

  public static void main(String[] args) throws Exception {

    String input = "file:///myprojects/big-data-flink/data/hello.txt";

    // step1: get the environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // step2: read data
    DataSource<String> text = env.readTextFile(input);

    // step3: transform
    text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
      @Override
      public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception{
        String[] tokens=value.toLowerCase().split("\t");
        for (String token : tokens){
          if (token.length() > 0){
            collector.collect(new Tuple2<String, Integer>(token, 1));
          }
        }
      }
    }).groupBy(0).sum(1).print();
  }
}
