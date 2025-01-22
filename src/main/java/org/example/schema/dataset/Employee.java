package org.example.schema.dataset;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;

/**
 * @author ankush.nakaskar
 */
public   class Employee implements Serializable {
    private String name;
    private BigInteger age;
    private List<BigInteger> marks;

    public Employee(String name, BigInteger age, List<BigInteger> marks){
        this.name=name;
        this.age=age;
        this.marks = marks;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(BigInteger age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public BigInteger getAge() {
        return age;
    }

    public List<BigInteger> getMarks() {
        return marks;
    }

    public void setMarks(List<BigInteger> marks) {
        this.marks = marks;
    }
}
