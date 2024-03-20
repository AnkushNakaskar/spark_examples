package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("[']*[0-9].*");
        String input ="'407084330414";
        Matcher matcher = pattern.matcher(input);
        List<String> matches = new ArrayList<>();
        while (matcher.find()) {
            matches.add(matcher.group());
        }
        System.out.println(matches);
        String value = input.replace("\'", " ");
        System.out.println(value);

    checkString();

    }

    private static void checkString() {

        Pattern pattern = Pattern.compile("w3schools");
        String input ="Visit w3schools!";
        Matcher matcher = pattern.matcher(input);
        List<String> matches = new ArrayList<>();
        while (matcher.find()) {
            matches.add(matcher.group());
        }
        System.out.println(matches);
    }


}
