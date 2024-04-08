package org.example;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) throws ParseException {
        System.out.println("hascodemap");
        extracted();
        extracted1();
        extracted2();
    }

    private static void extracted2() throws ParseException {
        String inputPattern ="00(\\d{1}\\d{4}\\d{2}\\d{2})00_PP01";
        String settlementCyclePattern = "\\d{1}";
        String dateFormat ="yyyy-MM-dd";
        final Pattern pattern = Pattern.compile(inputPattern);
        final Matcher m = pattern.matcher("0012024021300_PP01");
        if (m.find()) {
            String patternDateString=m.group(1);
            final Pattern cyclePattern = Pattern.compile(settlementCyclePattern);
            final Matcher cycleMatcher = cyclePattern.matcher(patternDateString);
            if(cycleMatcher.find()){
                String cycle =cycleMatcher.group(0);
                System.out.println(cycle);
                System.out.println(cycleMatcher.start());
                System.out.println(cycleMatcher.end());
            }
            Date actualDate = new SimpleDateFormat(dateFormat).parse(patternDateString);
            System.out.println("Match found");
        }
        else {
            System.out.println("Match not found");
        }
    }


    private static void extracted() {
        Pattern pattern = Pattern.compile("^(EVDPhonepe_RechargeTransactions_).*", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher("EVDPhonepe_RechargeTransactions_2023-06-30.csv");
        boolean matchFound = matcher.find();
        if(matchFound) {
            System.out.println("Match found");
        } else {
            System.out.println("Match not found");
        }
    }

    private static void extracted1() {
        Pattern pattern = Pattern.compile("^(00)([0-4])([0-4]{4})([0-9]{2})([0-9]{2})00_PP01.*", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher("0012024021300_PP01.zip");
        boolean matchFound = matcher.find();
        if(matchFound) {
            System.out.println("Match found");
        } else {
            System.out.println("Match not found");
        }
    }

}
