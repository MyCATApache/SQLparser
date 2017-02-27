package io.mycat;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

import static io.mycat.TokenHash.FROM;

/**
 * Created by Kaiz on 2017/2/6.
 *
 * 2017/2/24
 * 表名规范遵循 ： https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
 *                 暂时不支持字符型表名
 *                 https://github.com/mysql/mysql-workbench/blob/1972008552c725c4176fb83a35734cf2c1f6158c/library/parsers/grammars/MySQL.g
 * 2017/2/25
 * SQLBenchmark.DruidTest          thrpt   10   225009.114 ±  4904.006  ops/s
 * SQLBenchmark.NewSqQLParserTest  thrpt   10  3431639.918 ± 54679.339  ops/s
 * SQLBenchmark.SQLParserTest      thrpt   10  1297501.144 ± 56213.315  ops/s
 * 2017/2/26
 * 以 -- 开头的注释直接跳过
 * sql内字符串只保留字符串标识，第二阶段需要的话再根据pos和size进行解析
 * sql内数值只保留数值标识，第二阶段需要的话再根据pos和size进行解析
 * hash数组会保留两种内容，一种是标志位(<41)标识当前位置是什么东西，比如标点符号或者字符串或者数值等等，主要为第二阶段解析提供协助，尽量避免第二阶段需要重新遍历字串
 * 另一种是哈希值(>=41)，主要是sql关键字、表名、库名、别名，部分长度超过12个字符的有可能发生哈希值碰撞，这时可以先比较size是否一致，然后进行逐字符匹配
 * 计划用建立512大小的关键字哈希索引数组，最长碰撞为5，数组有效数量是379（ & 0x3fe >> 1）
 *
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
            pos = 16;
        };
        void set(long hash) { hashArray[pos++] = hash; }
        long get(int idx) { return hashArray[idx]; }
        int getCount() {return pos-16;}
    }

    class TokenArray {
        short[] tokenPosArray = new short[1024];
        byte[] tokenSizeArray = new byte[1024];
        int pos = 0;

        void init() {
            while(pos>=0) {
                tokenPosArray[pos] = 0;
                tokenSizeArray[pos--] = 0;
            }
            pos = 0;
        }

        void setPos(short tokenPos) { tokenPosArray[pos] = tokenPos;}
        void setSize(byte size) { tokenSizeArray[pos++] = size;}
    }

//    final byte readArrayByte(Object array, long pos) { return unsafe.getByte(array, pos+16); }//unsafe访问数组需要往后偏移16位？？

    final byte DIGITS = 1;
    final byte CHARS = 2;
    final byte DOT = 3;
    final byte COMMA = 4;
    final byte STRINGS = 5;
    final byte BACK_SLASH = 6;
    final byte LEFT_PARENTHESES = 7;
    final byte RIGHT_PARENTHESES = 8;
    final byte SEMICOLON = 9;
    final byte MINUS = 10;
    final byte DIVISION = 11;
    final byte STAR = 12;
    final byte EQUAL = 13;
    final byte PLUS = 14;
    final byte LESS = 15;
    final byte GREATER = 16;
    final byte AT = 17;

//    static final byte

    final byte[] charType = new byte[255];
    final byte[] shrinkCharTbl = new byte[96];//为了压缩hash字符映射空间，再次进行转义
    HashArray hashArray = new HashArray();
    TokenArray tokenArray = new TokenArray();
//    Unsafe unsafe;

    void init() {
        //// TODO: 2017/2/21 可能需要调整顺序进行优化
        IntStream.range('0', '9').forEach(c -> charType[c] = DIGITS);
        IntStream.range('A', 'Z').forEach(c -> charType[c] = CHARS);
        IntStream.range('a', 'z').forEach(c -> charType[c] = CHARS);
        charType['_'] = CHARS;
        charType['$'] = CHARS;
        charType['.'] = DOT;
        charType[','] = COMMA;
        //字符串
        charType['"'] = STRINGS;
        charType['\''] = STRINGS;
        charType['\\'] = BACK_SLASH;
        //sql分隔
        charType['('] = LEFT_PARENTHESES;
        charType[')'] = RIGHT_PARENTHESES;
        charType[';'] = SEMICOLON;
        //（可能的）注释和运算符
        charType['-'] = MINUS;
        charType['/'] = DIVISION;
        charType['*'] = STAR;
        charType['='] = EQUAL;
        charType['+'] = PLUS;
        charType['<'] = LESS;
        charType['>'] = GREATER;
        charType['@'] = AT;

        shrinkCharTbl[0] = 1;//从 $ 开始计算
        IntStream.rangeClosed('0', '9').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'0'+2));
        IntStream.rangeClosed('A', 'Z').forEach(c -> shrinkCharTbl[c-'$'] = (byte)(c-'A'+12));
        shrinkCharTbl['_'-'$'] = (byte)38;

        // 通过反射得到theUnsafe对应的Field对象
//        Field field = null;
//        try {
//            field =Unsafe.class.getDeclaredField("theUnsafe");
//            // 设置该Field为可访问
//            field.setAccessible(true);
//            // 通过Field得到该Field对应的具体对象，传入null是因为该Field为static的
//            unsafe = (Unsafe) field.get(null);
//        } catch (NoSuchFieldException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        }
    }

    short parseToken(byte[] sql, short pos, int sqlLength) {
        byte size = 1;
        byte c = (byte)(sql[pos] & 0xDF);
        tokenArray.setPos(pos);
        long hash = shrinkCharTbl[c - '$'];
        while (++pos < sqlLength && (charType[c = sql[pos]] == 2) ) {
            hash = (hash*41)+shrinkCharTbl[((c & 0xDF) - '$')];//别问我为什么是41
            size++;
        }
        hashArray.set(hash);
        tokenArray.setSize(size);
        return pos;
    }

    short parseString(byte[] sql, short pos, int sqlLength, byte startSign) {
        byte size = 1;
        hashArray.set(STRINGS);
        tokenArray.setPos((short)(pos-1));
        byte c;
        while (++pos < sqlLength ) {
            c = sql[pos];
            if (c == '\\') {
                pos+=2;
            } else if (c == startSign ) {
                break;
            } else {
                size++;
            }
        }
        tokenArray.setSize(size);
        return pos;
    }

    void tokenize(byte[] sql) {
        short pos = 0;
        int sqlLength = sql.length;
        hashArray.init();
        tokenArray.init();
        byte c;
        byte cType;
        while (pos < sqlLength) {
            c = sql[pos];
            cType = charType[c];
            if (cType == CHARS) {
                pos = parseToken(sql, pos, sqlLength);
            } else if (cType == DIGITS) {
                hashArray.set(DIGITS);
                tokenArray.setPos(pos);
                byte size = 1;
                while (++pos<sqlLength && charType[sql[pos]] == DIGITS) {
                    size++;
                }
                tokenArray.setSize(size);
            } else if (cType == MINUS) {
                if (charType[sql[++pos]]!=MINUS) {
                    hashArray.set(MINUS);
                    tokenArray.setPos(pos--);
                    tokenArray.setSize((byte)1);
                } else {
                    while (++pos < sqlLength && sql[pos]!='\n'); //跳过单行注释
                }
            } else if (cType == STRINGS) {
                pos = parseString(sql, ++pos, sqlLength, c);
            } else if (cType != 0) {
                hashArray.set(cType);
                tokenArray.setPos(pos++);
                tokenArray.setSize((byte)1);
            } else {
                pos++;
            }
        }
    }

    /*
    * 用于进行第一遍处理，处理sql类型以及提取表名
     */
    public void firstParse(byte[] sql, SQLContext context) {
        tokenize(sql);
        for(int i=0; i<hashArray.getCount(); i++) {
            switch (hashArray.get(i)) {
                case FROM:
            }
        }

    }

    /*
    * 计划用于第二遍解析，处理分片表分片条件
    */
    public void secondParse() {

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
        byte[] src = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));".getBytes(StandardCharsets.UTF_8);//20个token
//        long min = 0;
//        for (int i = 0; i < 50; i++) {
//            System.out.print("Loop " + i + " : ");
//            long cur = RunBench(src, parser);//不加分析应该可以进2.6秒
//            System.out.println(cur);
//            if (cur < min || min == 0) {
//                min = cur;
//            }
//        }
//        System.out.print("min time : " + min);
        parser.tokenize(src);
        System.out.print("token count : "+parser.hashArray.getCount());
    }
}
