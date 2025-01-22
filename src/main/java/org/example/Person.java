package org.example;

import java.io.Serializable;
import java.util.List;

/**
 * @author ankush.nakaskar
 */
public   class Person implements Serializable {
    private String name;
    private Integer age;

    private List<Integer> marks;

    public Person(String name, Integer age, List<Integer> marks){
        this.name=name;
        this.age=age;
        this.marks = marks;
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

    public List<Integer> getMarks() {
        return marks;
    }

    public void setMarks(List<Integer> marks) {
        this.marks = marks;
    }
}
