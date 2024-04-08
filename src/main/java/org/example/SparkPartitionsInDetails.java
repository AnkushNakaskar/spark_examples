package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkPartitionsInDetails {

    public static void main(String[] args) {
        SparkPartitionsInDetails df =new SparkPartitionsInDetails();
        df.start();
    }
    public void start(){
        int numberOfThrows =100;
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Pi")
                .master("local[*]")
                .getOrCreate();
        List<Integer> list = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            list.add(i);
        }
        Dataset<Row> incrementalDf = spark
                .createDataset(list, Encoders.INT())
                .toDF();
        incrementalDf.show(100);
        incrementalDf.printSchema();

        incrementalDf.filter(new SparkPartitionsInDetails.EvenNumberFilter()).show(5);
        System.out.println("Number of partitions111 : "+incrementalDf.toJavaRDD().getNumPartitions());
        System.out.println("Partitioner111 : "+incrementalDf.toJavaRDD().partitioner());
        System.out.println("Partitions structure1111 : "+incrementalDf.toJavaRDD().glom().collect());
        List<List<Row>> gloomValues = incrementalDf.toJavaRDD().glom().collect();

        for(List<Row> row : gloomValues){
            System.out.println("Partition Values length111 : "+row.size());
        }
        Dataset<Row> valuesUpdated = incrementalDf.repartition(1);

        List<List<Row>> gloomValuesNew = valuesUpdated.toJavaRDD().glom().collect();

        System.out.println("Number of partitions : "+valuesUpdated.toJavaRDD().getNumPartitions());
        System.out.println("Partitioner : "+incrementalDf.toJavaRDD().partitioner());
        System.out.println("Partitions structure : "+incrementalDf.toJavaRDD().glom().collect());

        System.out.println("availableProcessors / Cores in machine : "+Runtime.getRuntime().availableProcessors());
        for(List<Row> row : gloomValuesNew){
            System.out.println("Partition Values length : "+row.size());
        }

        Dataset<Row> valuesUpdatedName = incrementalDf.repartition(col("value"));
        List<List<Row>> gloomValuesNewName = valuesUpdatedName.toJavaRDD().glom().collect();

        System.out.println("Number of partitionsName : "+valuesUpdatedName.toJavaRDD().getNumPartitions());
        System.out.println("PartitionerName : "+valuesUpdatedName.toJavaRDD().partitioner());
        System.out.println("Partitions structureName : "+valuesUpdatedName.toJavaRDD().glom().collect());

        for(List<Row> row : gloomValuesNewName){
            System.out.println("Partition Values length Name: "+row.size());
        }

    }

    static  class EvenNumberFilter implements FilterFunction<Row> {
        private static final long serialVersionUID=1L;

        @Override
        public boolean call(Row row) throws Exception {
            int input = row.getInt(0);
            return input%2==0;
        }
    }
}
