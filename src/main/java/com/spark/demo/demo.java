package com.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class demo {
    private static String appName = "spark.demo";
    private static String master = "local[*]";

    /**
     * 计算数组之和
     *
     * @param args
     */
    public static void main(String[] args) {
        JavaSparkContext sc = null;
        try {
            //初始化 JavaSparkContext
            SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            sc = new JavaSparkContext(conf);

            // 构造数据源
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 20);

            //并行化创建rdd
            JavaRDD<Integer> rdd = sc.parallelize(data);

            //map && reduce
            Integer result = rdd.map(new Function<Integer, Integer>() {
                @Override
                public Integer call(Integer integer) throws Exception {
                    return integer;
                }
            }).reduce(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer o, Integer o2) throws Exception {
                    return o + o2;
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

