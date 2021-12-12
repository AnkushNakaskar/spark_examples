package org.example;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * @author ankush.nakaskar
 * @version 1.0
 * @date 12/12/21 10:29 AM
 */
public class JsonIngestionInSparkExample {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .appName("Json ingestion example")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = session.read().format("json")
                .load("data/Restaurants_in_Durham_County_NC.json");
        df.show(5);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");
        System.out.println(" Start the transformation....!!!!");
        df = df.withColumn("country", functions.lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type",functions.split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));

        df = df.withColumn("id", functions.concat(df.col("state"), functions.lit("_"),
                df.col("country"), functions.lit("_"),
                df.col("datasetId"))); System.out.println("*** Dataframe transformed"); df.show(5);
        df.printSchema();

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length; System.out.println("Partition count before repartition: " +
                partitionCount);
        df = df.repartition(4);
        System.out.println("Partition count after repartition: " +
                df.rdd().partitions().length);

    }
}
