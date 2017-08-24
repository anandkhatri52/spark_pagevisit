package com.mobiquityinc.spark.funcation;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * Created by anandkhatri on 6/29/15.
 */
public class FetchIPAddress implements FlatMapFunction<String, String> {

    public Iterable<String> call(String s) throws Exception {
        System.out.println(">>>> Normal Textfile content >>>> "+s);
        return Arrays.asList(s.substring(s.lastIndexOf(",") + 1));
    }
}
