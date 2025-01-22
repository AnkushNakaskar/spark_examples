package org.example.schema.dataset;


import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * @author ankush.nakaskar
 */
public class SparkJsonFileToJavaObject {

    public static void main(String[] args) throws JsonProcessingException {

        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample application")
                .master("local")
                .getOrCreate();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        Dataset<Employee> ds = spark.read().json("./data/employee_object_dataset.json").as(employeeEncoder);
        ds.show(5);



    }


}
