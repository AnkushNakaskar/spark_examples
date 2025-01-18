package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ankush.nakaskar
 */
public class SchemaExercise {

    public static void main(String[] args) throws JsonProcessingException {

        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample application")
                .master("local")
                .getOrCreate();
//


        StructType companySchema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);



        String jsonString =getJsonString();
       System.out.println(jsonString);
//       Exception in thread "main" org.apache.spark.sql.AnalysisException: Path does not exist: file:/Users/ankush.nakaskar/Office/newCode/spark_examples/person.json
        Dataset<Row> df = spark.read().schema(companySchema).json("./data/person.json").toDF();
//        df.show();
        df.show(10);


    }

    private static String getJsonString() throws JsonProcessingException {
        ObjectMapper mapper =new ObjectMapper();
        List<Person> personList =new ArrayList<>();
        personList.add(new Person("Ankush",21));
        personList.add(new Person("Ankush1",22));
        personList.add(new Person("Ankush2",23));
        personList.add(new Person("Ankush3",24));
        personList.add(new Person("Ankush4",25));
        String result = mapper.writeValueAsString(personList);
        return result;
    }
    static class Person implements Serializable {
        private String name;
        private Integer age;

        public Person(String name, Integer age){
            this.name=name;
            this.age=age;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public Integer getAge() {
            return age;
        }

    }

    private static StructType getSchema() {
        String schemaString = "name,age";
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
