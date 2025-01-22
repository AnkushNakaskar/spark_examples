package org.example.schema.dataset;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;

/**
 * @author ankush.nakaskar
 */
public   class Employee implements Serializable {
    private String name;
    private Integer age;

    public Employee(){

    }

    public Employee(String name, Integer age){
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
