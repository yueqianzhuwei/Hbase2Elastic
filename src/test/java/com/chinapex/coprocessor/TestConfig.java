//package com.chinapex.coprocessor;//package com.chinapex;
//
//import java.io.InputStream;
//import java.util.Enumeration;
//import java.util.Properties;
//
///**
// * Created by Administrator on 2018/9/25.
// */
//public class TestConfig {
//    public static void main(String[] args) {
//
//        Properties props = readProperties("configure.properties");
//    }
//
//    public static Properties readProperties(String filePath) {
//        Properties props = new Properties();
//        try {
//            InputStream stream = TestConfig.class.getClassLoader().getResourceAsStream(filePath);
//            props.load(stream);
//            Enumeration en = props.propertyNames();
//            while (en.hasMoreElements()) {
//                String key = (String) en.nextElement();
//                String Property = props.getProperty(key);
//                System.out.println(key +","+ Property);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return props;
//    }
//}
