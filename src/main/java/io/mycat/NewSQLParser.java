package io.mycat;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

/**
 * Created by Kaiz on 2017/2/6.
 */
public class NewSQLParser {
    SQLContext context;
    SQLReader reader;

    class HashArray {
        long[] hashArray = new long[1024];
        int pos = 0;

        void init() {
            while(pos>=0) {
                hashArray[pos--] = 0;
            }
            pos = 0;
        };
        void set(long hash) { hashArray[pos++] = hash; }
        long get(int idx) { return hashArray[idx]; }
        int getCount() {return pos;}
    }

    final byte[] charType = new byte[128];
    final short[] tokenPos = new short[1024];
    HashArray hashArray = new HashArray();

    void init() {
        IntStream.range('A', 'Z').forEach(c -> charType[c] = 1);
        IntStream.range('a', 'z').forEach(c -> charType[c] = 1);
        charType['_'] = 1;
    }

    int parseToken(byte[] sql, int pos, int sqlLength) {
        int tp = 0;
        byte c = (byte)(sql[pos] & 0xDF);
        long hash = c;
        pos++;
        while (pos < sqlLength && charType[c = sql[pos]] == 1) {
            hash += (long)(c & 0xDF) << (tp & 0xFF);
            pos++;
            tp++;
        }
        hashArray.set(hash);
        return pos;
    }

    void tokenize(byte[] sql) {
        int sqlLength = sql.length;
        int pos = 0;
        hashArray.init();
        while (pos < sqlLength) {
            if (charType[sql[pos]]==1) {
                pos = parseToken(sql, pos, sqlLength);
            } else {
                pos++;
            }
        }
    }

    static long RunBench(byte[] src, NewSQLParser parser) {
        int count = 0;
        long start = System.currentTimeMillis();
        do {
            parser.tokenize(src);
        } while (count++ < 10_000_000);
        return System.currentTimeMillis() - start;
    }

    public static void main(String[] args) {
        NewSQLParser parser = new NewSQLParser();
        parser.init();
        long min = 0;
        byte[] src = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));".getBytes(StandardCharsets.UTF_8);//20个token
        for (int i = 0; i < 50; i++) {
            System.out.print("Loop " + i + " : ");
            long cur = RunBench(src, parser);//by kaiz : 不加分析应该可以进2.6秒
            System.out.println(cur);
            if (cur < min || min == 0) {
                min = cur;
            }
        }
        System.out.print("min time : " + min);
//        parser.tokenize(src);
//        System.out.print("token count : "+parser.hashArray.getCount());
    }
}
