//package com.chinapex.coprocessor;
//
//import java.lang.reflect.Field;
//
///**
// * Created by Administrator on 2018/9/23.
// */
//public class PriObj {
//    private User user;
//
//    private String aaa;
//
//    public String getAaa() {
//        return aaa;
//    }
//
//    public void setAaa(String aaa) {
//        this.aaa = aaa;
//    }
//
//    public User getUser() {
//        return user;
//    }
//
//    public void setUser(User user) {
//        this.user = user;
//    }
//
//    public static void main(String[] args) throws Exception{
//        PriObj oo=new PriObj();
//        User uu=new User();
//        uu.setName("zhuwei");
//        uu.setPwd("111");
//        oo.setUser(uu);
//
//        Field filed = oo.getClass().getDeclaredField("user");
//        System.out.println(filed);
//    }
//
//
//    public static Object getValue(Object target, String fieldName) {
//        Class<?> clazz = target.getClass();
//        String[] fs = fieldName.split("\\.");
//        try{
//            for(int i = 0;i < fs.length - 1;i++) {
//                Class<?>[] classes = clazz.getDeclaredClasses();
//                classes[0].getDeclaredMethod("numberOfActions");
//                Field f = clazz.getDeclaredField(fs[i]);
//                f.setAccessible(true);
//                target= f.get(target);
//                clazz = target.getClass();
//            }
//            Field f = clazz.getDeclaredField(fs[fs.length - 1]);
//            f.setAccessible(true);
//            return f.get(target);
//        }catch(Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//}
