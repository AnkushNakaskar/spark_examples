package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import org.apache.spark.sql.functions.*;
/**
 * @author ankush.nakaskar
 * @version 1.0
 * @date 25/01/22 8:20 AM
 */
public class DataFrameDividedBy2 {
    public static void main(String[] args) {
        DataFrameDividedBy2 df =new DataFrameDividedBy2();
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
//        incrementalDf.filter((FilterFunction<Row>) row -> row.getInt(0)%2==0).show(5);
        incrementalDf.filter(new EvenNumberFilter()).show(5);

    }

    static  class EvenNumberFilter implements  FilterFunction<Row> {
        private static final long serialVersionUID=1L;

        @Override
        public boolean call(Row row) throws Exception {
            int input = row.getInt(0);
            return input%2==0;
        }
    }
}
