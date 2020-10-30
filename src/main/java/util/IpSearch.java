package util;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IpSearch {
    private static IpSearch instance = null;

    static {
        if (null == instance)
            instance = new IpSearch();
    }

    private byte[] data;

    private HashMap<Integer, PrefixIndex> prefixMap;

    private long firstStartIpOffset;// 索引区第一条流位置
    // private int lastStartIpOffset;//索引区最后一条流位置
    private long prefixStartOffset;// 前缀区第一条的流位置
    private long prefixEndOffset;// 前缀区最后一条的流位置
    // private int ipCount; //ip段数量
    private long prefixCount; // 前缀数量

    public IpSearch() {

//        Path path = Paths
//                .get("D:\\QQFile\\272679705\\FileRecv\\file\\qqzeng-ip-utf8.dat");


        Path path = Paths
                .get("/home/tool/qqzeng-ip-utf8.dat");


        try {
            data = Files.readAllBytes(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        firstStartIpOffset = BytesToLong(data[0], data[1], data[2], data[3]);
        // lastStartIpOffset = BytesToLong(data[4], data[5], data[6], data[7]);
        prefixStartOffset = BytesToLong(data[8], data[9], data[10], data[11]);
        prefixEndOffset = BytesToLong(data[12], data[13], data[14], data[15]);

        // ipCount = (lastStartIpOffset - firstStartIpOffset) / 12 + 1; //索引区块每组
        // 12字节
        prefixCount = (prefixEndOffset - prefixStartOffset) / 9 + 1; // 前缀区块每组
        // 9字节

        // 初始化前缀对应索引区区间
        byte[] indexBuffer = Arrays.copyOfRange(data, (int) prefixStartOffset, (int) prefixEndOffset + 9);
        prefixMap = new HashMap<Integer, PrefixIndex>();
        for (int k = 0; k < prefixCount; k++) {
            int i = k * 9;
            int prefix = (int) (indexBuffer[i] & 0xFFL);

            PrefixIndex pf = new PrefixIndex();
            pf.start_index = BytesToLong(indexBuffer[i + 1], indexBuffer[i + 2], indexBuffer[i + 3], indexBuffer[i + 4]);
            pf.end_index = BytesToLong(indexBuffer[i + 5], indexBuffer[i + 6], indexBuffer[i + 7], indexBuffer[i + 8]);
            prefixMap.put(prefix, pf);

        }

    }

    public String Get(String ip) {
        String[] ips = ip.split("\\.");
        int prefix = Integer.valueOf(ips[0]);
        long intIP = ipToLong(ip);

        long high = 0;
        long low = 0;

        if (prefixMap.containsKey(prefix)) {
            low = prefixMap.get(prefix).start_index;
            high = prefixMap.get(prefix).end_index;

        } else {
            return "";
        }

        long my_index = low == high ? low : BinarySearch(low, high, intIP);

        IpIndex ipindex = new IpIndex();
        GetIndex((int) my_index, ipindex);

        if ((ipindex.startip <= intIP) && (ipindex.endip >= intIP)) {
            return GetLocal(ipindex.local_offset, ipindex.local_length);
        } else {
            return "";
        }

    }

    // / <summary>
    // / 二分逼近算法
    // / </summary>
    public long BinarySearch(long low, long high, long k) {
        long M = 0;
        while (low <= high) {
            long mid = (low + high) / 2;

            long endipNum = GetEndIp(mid);
            if (endipNum >= k) {
                M = mid;
                if (mid == 0) {
                    break; // 防止溢出
                }
                high = mid - 1;
            } else
                low = mid + 1;
        }
        return M;
    }

    // / <summary>
    // / 在索引区解析
    // / </summary>
    // / <param name="left">ip第left个索引</param>
    private void GetIndex(int left, IpIndex ipindex) {
        int left_offset = (int) firstStartIpOffset + (left * 12);
        ipindex.startip = BytesToLong(data[left_offset], data[1 + left_offset], data[2 + left_offset], data[3 + left_offset]);
        ipindex.endip = BytesToLong(data[4 + left_offset], data[5 + left_offset], data[6 + left_offset], data[7 + left_offset]);
        ipindex.local_offset = (int) BytesToLong3(data[8 + left_offset], data[9 + left_offset], data[10 + left_offset]);
        ipindex.local_length = (int) data[11 + left_offset];
    }

    // / <summary>
    // / 只获取结束ip的数值
    // / </summary>
    // / <param name="left">索引区第left个索引</param>
    // / <returns>返回结束ip的数值</returns>
    private long GetEndIp(long left) {
        int left_offset = (int) firstStartIpOffset + (int) (left * 12);
        return BytesToLong(data[4 + left_offset], data[5 + left_offset], data[6 + left_offset], data[7 + left_offset]);

    }

    // / <summary>
    // / 返回地址信息
    // / </summary>
    // / <param name="local_offset">地址信息的流位置</param>
    // / <param name="local_length">地址信息的流长度</param>
    // / <returns></returns>
    private String GetLocal(int local_offset, int local_length) {
        byte[] bytes = new byte[local_length];
        bytes = Arrays.copyOfRange(data, local_offset, local_offset + local_length);
        return new String(bytes, StandardCharsets.UTF_8);

    }

    // / <summary>
    // / 字节转整形 小节序
    // / </summary>
    private long BytesToLong(byte a, byte b, byte c, byte d) {
        return (a & 0xFFL) | ((b << 8) & 0xFF00L) | ((c << 16) & 0xFF0000L) | ((d << 24) & 0xFF000000L);

    }

    private long BytesToLong3(byte a, byte b, byte c) {
        return (a & 0xFFL) | ((b << 8) & 0xFF00L) | ((c << 16) & 0xFF0000L);

    }

    public long ipToLong(String ip) {
        String[] quads = ip.split("\\.");
        long result = 0;
        result += Integer.parseInt(quads[3]);
        result += Long.parseLong(quads[2]) << 8L;
        result += Long.parseLong(quads[1]) << 16L;
        result += Long.parseLong(quads[0]) << 24L;
        return result;
    }

    public static Map getRegionByIp(String ip) {
        Map map = Maps.newHashMap();
        String result = instance.Get(ip);

        if (StringUtils.isNotBlank(result)) {
            String[] str = result.split("\\|");
            if (str != null && str.length > 10) {
                map.put("大洲", str[0]);
                map.put("国家", str[1]);
                map.put("省份", str[2]);
                map.put("城市", str[3]);
                map.put("区县", str[4]);
                map.put("运营", str[5]);
                map.put("区划", str[6]);
                map.put("英文", str[7]);
                map.put("简码", str[8]);
                map.put("经度", str[9]);
                map.put("纬度", str[10]);
            }
        }
        return map;
    }

    public static void main(String[] args) {

        String ip = "183.182.31.255";
        String result = instance.Get(ip);
        String[] str = result.split("\\|");

        Map map = Maps.newHashMap();
        map.put("大洲", str[0]);
        map.put("国家", str[1]);
        map.put("省份", str[2]);
        map.put("城市", str[3]);
        map.put("区县", str[4]);
        map.put("运营", str[5]);
        map.put("区划", str[6]);
        map.put("英文", str[7]);
        map.put("简码", str[8]);
        map.put("经度", str[9]);
        map.put("纬度", str[10]);

        System.out.println(map.get("运营").toString());
    }
}
