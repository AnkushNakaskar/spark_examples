package org.example;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        final List<String> strings = Arrays.asList("Owl", "Parrot", "Crow", "Ibis", "WoodPecker",
                "Bulbul", "Falcon",
                "Eagle", "Kite", "Toucan");

        Map<Integer,List<String>> hascodemap =new HashMap<>();
        for(String val : strings){
            int index = val.length()%4;
            List<String> listOfValues = hascodemap.getOrDefault(index, new ArrayList<>());
            listOfValues.add(val);
            hascodemap.put(index,listOfValues);
        }
        System.out.println(hascodemap);
//        {0=[Owl, Bulbul, Eagle], -1=[WoodPecker], -2=[Parrot, Toucan], 1=[Falcon], 3=[Crow, Ibis, Kite]}


        hascodemap =new HashMap<>();
        for(String val : strings){
            int index = val.length()%8;
            System.out.println(index);
            List<String> listOfValues = hascodemap.getOrDefault(index, new ArrayList<>());
            listOfValues.add(val);
            hascodemap.put(index,listOfValues);
        }
        System.out.println(hascodemap);

    }

}
