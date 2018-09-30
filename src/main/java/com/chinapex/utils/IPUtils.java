package com.chinapex.utils;

import sun.misc.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author: Juvie
 * @CreateDate: 2018/9/26 15:42
 * @UpdateUser: Juvie
 * @UpdateDate: 2018/9/26 15:42
 * @UpdateRemark: 修改内容
 * @Version: 1.0
 */
public class IPUtils {


    public static final String datapath="/assets/ip.data";

    public  synchronized Map<String,String> geoDataMap(String ip) {
        int loc = Integer.parseInt(String.valueOf(ip2Long(ip)));
        int country_id = (char) (this.readBytesFromResource())[loc] & 0xFF;
        int region_id = (char) this.readBytesFromResource()[(loc + 1)] & 0xFF;
        int cid1 = (char) this.readBytesFromResource()[(loc + 2)] & 0xFF;
        int cid2 = (char) this.readBytesFromResource()[(loc + 3)] & 0xFF;
        int cid = cid1 * 256 + cid2;


        Map<String,String> map=new HashMap<String,String>();
        map.put("country",IpGeoConst.country.get(country_id));
        map.put("region",IpGeoConst.region.get(region_id));
        map.put("city",IpGeoConst.city.get(cid));

        return map;
    }

    public static long ip2Long(String strip) {
        long ip[] = new long[4];
        int position1 = strip.indexOf(".");
        int position2 = strip.indexOf(".", position1 + 1);
        int position3 = strip.indexOf(".", position2 + 1);
        ip[0] = Long.parseLong(strip.substring(0, position1));
        ip[1] = Long.parseLong(strip.substring(position1 + 1, position2));
        ip[2] = Long.parseLong(strip.substring(position2 + 1, position3));
        ip[3] = Long.parseLong(strip.substring(position3 + 1));
        return ((ip[0] << 16) + (ip[1] << 8) + ip[2]) * 5;
    }

    public   byte[] readBytesFromResource() {
        try {
            InputStream stream = IPUtils.class.getResourceAsStream(IPUtils.datapath);
//            InputStream stream = new FileInputStream(new File(path));
            return IOUtils.readFully(stream, -1, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



}
