package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.ArrayList;
import java.util.List;

/*
    This program explains the accumulator in spark
    1. Accumulator is use when you have some counter and you can refer that in driver node.
        -> Like , in below example , we want to know , how many even number are there.
        -> My laptop has 10 partition because , there are 10 available processor.
        -> 1-100 : data is divided into 10 partition, hence every partition will have 5 even number.
        -> You can refer this : http://localhost:4040/stages/stage/?id=0&attempt=0
        -> at the end you can get the value 50 in total.
 */
public class SparkCollectStringAccumulatorsInDetails {

    public static void main(String[] args) {
        SparkCollectStringAccumulatorsInDetails df =new SparkCollectStringAccumulatorsInDetails();
        df.start();
    }
    public void start() {
        int numberOfThrows = 100;
        LongAccumulator accEven = new LongAccumulator();
        CollectionAccumulator<String> stringCollectionAccumulator =new CollectionAccumulator<>();
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Pi")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().register(accEven,"evenNumber");
        spark.sparkContext().register(stringCollectionAccumulator,"evenNumberList");
        List<Integer> list = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            list.add(i);
            list.add(i);
        }
        Dataset<Row> incrementalDf = spark
                .createDataset(list, Encoders.INT())
                .toDF();
//        incrementalDf.show(100);
        incrementalDf.printSchema();
        incrementalDf.foreach(row ->{
            Integer val = row.getInt(0);
            System.out.println(val);
            if(val%2==0){
                accEven.add(1);
                if(!stringCollectionAccumulator.value().contains(String.valueOf(val))){
                    stringCollectionAccumulator.add(String.valueOf(val));
                }
            }
        });
        try {
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println(">>>> : "+accEven.count());
        System.out.println(">>>> : "+stringCollectionAccumulator.value());
    }
}
