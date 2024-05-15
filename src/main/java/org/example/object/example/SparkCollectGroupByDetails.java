package org.example.object.example;

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








public class SparkCollectGroupByDetails {

    public static void main(String[] args) {
        SparkCollectGroupByDetails df =new SparkCollectGroupByDetails();
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
        ValidRowResponse validRowResponse1 =new ValidRowResponse();
        validRowResponse1.setContent("validRowResponse1_Content");
        validRowResponse1.setFileId("validRowResponse1_FileId");
        validRowResponse1.setKey("validRowResponse1_Key");
        validRowResponse1.setSchemaName("validRowResponse1_SchemaName");
        List<FailedValidationMessage> validRowResponse1Errors =new ArrayList<>();
        FailedValidationMessage validRowResponse1Failed = new FailedValidationMessage();
        validRowResponse1Failed.setField("validRowResponse1Failed_field");
        validRowResponse1Failed.setReason("validRowResponse1Failed_reason");
        validRowResponse1Failed.setValue("validRowResponse1Failed_Value");
        validRowResponse1Errors.add(validRowResponse1Failed);
        validRowResponse1.setErrors(validRowResponse1Errors);




        ValidRowResponse validRowResponse2 =new ValidRowResponse();
        validRowResponse2.setContent("validRowResponse2_Content");
        validRowResponse2.setFileId("validRowResponse2_FileId");
        validRowResponse2.setKey("validRowResponse2_Key");
        validRowResponse2.setSchemaName("validRowResponse2_SchemaName");
        List<FailedValidationMessage> validRowResponse2Errors =new ArrayList<>();
        FailedValidationMessage validRowResponse2Failed = new FailedValidationMessage();
        validRowResponse2Failed.setField("validRowResponse2Failed_field");
        validRowResponse2Failed.setReason("validRowResponse2Failed_reason");
        validRowResponse2Failed.setValue("validRowResponse2Failed_Value");
        validRowResponse2Errors.add(validRowResponse2Failed);
        validRowResponse2.setErrors(validRowResponse2Errors);
        List<ValidRowResponse> list = new ArrayList<>(numberOfThrows);
        list.add(validRowResponse1);
        list.add(validRowResponse2);
        Dataset<Row> incrementalDf = spark
                .createDataset(list, Encoders.bean(ValidRowResponse.class))
                .toDF();

        incrementalDf.show(10);
        incrementalDf.printSchema();





//        List<Integer> list = new ArrayList<>(numberOfThrows);
//        for (int i = 0; i < numberOfThrows; i++) {
//            list.add(i);
//            list.add(i);
//        }
//        Dataset<Row> incrementalDf = spark
//                .createDataset(list, Encoders.INT())
//                .toDF();
//        incrementalDf.show(10);
//        incrementalDf.printSchema();
//        Dataset<Row> countDF = incrementalDf.groupBy("value").count();
//        countDF.show(10);
    }
}
