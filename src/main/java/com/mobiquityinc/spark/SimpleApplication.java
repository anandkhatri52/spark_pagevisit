package com.mobiquityinc.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.mobiquityinc.spark.entity.PageVisitCount;
import com.mobiquityinc.spark.funcation.FetchIPAddress;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by anand on 6/16/15.
 */
public class SimpleApplication implements Serializable{

    private static final Pattern COMMA = Pattern.compile(",");
    private EntityManager entityManager;

    public static void main(String[] args) {

        String hdfsDirPath = "hdfs://ip-172-31-8-67:8020/kafka/kfh_topic/15-06-25/*"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("PageVisit Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileContent = sc.textFile(hdfsDirPath).cache();

        new SimpleApplication().aggregatePageVisit(fileContent);
        sc.stop();
    }

    private void aggregatePageVisit(JavaRDD<String> fileContent){

        JavaRDD<String> ipAddress  = fileContent.flatMap(new FetchIPAddress());

        JavaPairRDD<String, Integer> ones = ipAddress.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        System.out.println("Pair Map >>>>>> "+ones.toArray().toString());

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        System.out.println("counts >>>> "+ counts.toArray().toString());

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
            PageVisitCount visitCount = new PageVisitCount();
            visitCount.setIpAddress(tuple._1().toString());
            visitCount.setVisitCount((Integer)tuple._2());
            getEntityManager().persist(visitCount);
        }
    }

    private EntityManager getEntityManager(){
        if(entityManager == null){
            Map<String, String> propertyMap = new HashMap<String, String>();
            propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
            EntityManagerFactory emf = Persistence.createEntityManagerFactory("cassandra_pu", propertyMap);
            entityManager = emf.createEntityManager();
        }
        return entityManager;
    }
}

