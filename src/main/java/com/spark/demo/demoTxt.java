package com.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class demoTxt {
    private static String appName = "spark.demo";
    private static String master = "local[*]";

    /**
     * 读取txt文件，计算包含【spark】的每一行字符长度之和
     * @param args
     */
    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {
            //初始化 JavaSparkContext
            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            sc = new JavaSparkContext(conf);

            JavaRDD<String> rdd = sc.textFile("/Users/king/Downloads/spark.txt");

            //过滤
            rdd = rdd.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    return s.contains("spark");
                }
            });

            //map && reduce
            Integer result = rdd.map(new Function<String, Integer>() {
                @Override
                public Integer call(String s) throws Exception {
                    return s.length();
                }
            }).reduce(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            });
            System.out.println("执行结果：" + result);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}
