package com.chinapex.utils;/**
 * Created by Administrator on 2018/9/29.
 */

import java.util.Map;

/**
 * @Description:
 * @Author: Juvie
 * @CreateDate: 2018/9/29 18:17
 * @UpdateUser: Juvie
 * @UpdateDate: 2018/9/29 18:17
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class AAAA {

    public static void main(String[] args) {
        IPUtils aa=new IPUtils();
        Map<String, String> stringStringMap =aa.geoDataMap("122.144.218.13");
//        System.out.println(stringStringMap.get("country"));
    }
}
