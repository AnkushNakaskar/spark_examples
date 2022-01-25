package org.example;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ankush.nakaskar
 * @version 1.0
 * @date 12/12/21 9:52 AM
 */

public class UnionExampleOfRestaurants {

    public SparkSession session;

    public UnionExampleOfRestaurants(SparkSession session) {
        this.session = session;
    }


    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .appName("Union example of restaurants")
                .master("local")
                .getOrCreate();

        UnionExampleOfRestaurants example = new UnionExampleOfRestaurants(session);
        Dataset<Row> csvdf = example.readCSVFile(session);
        Dataset<Row> jsonDF = example.readJsonFile(session);
        example.combineDataframes(csvdf, jsonDF);

    }

    private Dataset<Row> readCSVFile(SparkSession session) {
        Dataset<Row> df = session.read().format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County.csv");
        df.show(5);
        df.printSchema();

        df = df.withColumn("county", functions.lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");
        df = df.withColumn("id", functions.concat(
                df.col("state"),
                functions.lit("_"),
                df.col("county"), functions.lit("_"),
                df.col("datasetId")));

        System.out.println("Showing CSV files data......");
        df.show(5);
        Partition[] partitions = df.rdd().partitions();
        System.out.println("Partions are  ::: " + partitions.length);
        return df;
    }

    private Dataset<Row> readJsonFile(SparkSession session) {



        Dataset<Row> df = session.read().format("json")
                .load("data/Restaurants_in_Durham_County_NC.json");
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");
        System.out.println(" Start the transformation....!!!!");
        df = df.withColumn("county", functions.lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
//                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type", functions.split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1))
                .drop(df.col("fields"))
                .drop(df.col("geometry"))
                .drop(df.col("record_timestamp"))
                .drop(df.col("fields.closing_date"))
                .drop(df.col("recordid"));

        df = df.withColumn("id", functions.concat(df.col("state"), functions.lit("_"),
                df.col("county"), functions.lit("_"),
                df.col("datasetId")));
        System.out.println("Showing json files data......");
        df.show(5);
        df.printSchema();

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " +
                partitionCount);
        df = df.repartition(4);
        System.out.println("Partition count after repartition: " +
                df.rdd().partitions().length);
        return df;
    }

    private void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
        Dataset<Row> df = df1.unionByName(df2);
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count: " + partitionCount);
    }
}
