package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jgp
 */
public class CsvToDataframeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    CsvToDataframeApp app = new CsvToDataframeApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Ankush Sample application")
        .master("local")
        .getOrCreate();

    StructType schema = getSchema();


    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    //Assigning the values in schema(Strict way)
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
            .option("skipRows", 1) // # Skip the specified number of rows after header

            .schema(schema)

        .load("data/books.csv");

    // Shows at most 5 rows from the dataframe
    df.show();
    df.printSchema();
    Row[] take = (Row[]) df.take(5); // this is same like prediction of schema if not specified in read/load method
    System.out.println(take);

    try {
      Thread.sleep(35000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private StructType getSchema() {
    String schemaString = "id,authorId,title,releaseDate,link";
    // Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(",")) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);
    return schema;
  }
}
