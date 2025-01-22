package org.example.schema.dataset;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;

/**
 * @author ankush.nakaskar
 */
public class SparkReadJsonRowToObject {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample Json read in Row application")
                .master("local")
                .getOrCreate();

        Dataset<Row> dataframe = spark.read().json("./data/employee_object_dataset.json");
        dataframe.show(5);
        List<Employee> countList = dataframe
                .map(new RowObjectToEmployeeMapper(), Encoders.bean(Employee.class))
                .collectAsList();
        System.out.println(countList);

    }

    static class RowObjectToEmployeeMapper implements MapFunction<Row, Employee>, Serializable {

        @Override
        public Employee call(Row row) {
            Employee employee =new Employee();
            employee.setName(row.getAs("name"));
            Long value = row.getAs("age");
            employee.setAge(value.intValue());
            return employee;
        }


    }

    static class StringObjectToEmployeeMapper implements MapFunction<String, String> {
        private static final long serialVersionUID = 38446L;


        @Override
        public String call(String input) throws Exception {
            String[] values = input.split(",");
            return "";
        }
    }

}
