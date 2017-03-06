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
            pos = 0;
        }
        void set(int type, int start, int size) { hashArray[pos++] = (long)type << 32 | size << 16 | start; pos++; }
        void set(int type, int start, int size, long hash) { hashArray[pos++] = (long)type << 32 | size << 16 | start; hashArray[pos++] = hash; }
        int getPos(int idx) { return ((int)hashArray[idx<<1])>>>16; }
        int getSize(int idx) { return ((int)hashArray[idx<<1]&0xFFFF0000); }
        int getType(int idx) { return (int)(hashArray[idx<<1]&0xFFFFFFFF00000000L>>>32); }
        void setType(int idx, int type) { hashArray[idx<<1] = (hashArray[idx<<1] & 0xFFFFFFFFL) | ((long)type << 32); }
        long getHash(int idx) { return hashArray[(idx<<1)+1]; }
        int getIntHash(int idx) {
            long value = hashArray[idx << 1];
            return (int)(value >>> 16);
        }
        int getCount() {return pos>>1;}
    }

/*    class TokenArray {
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
    }*/

//    final byte readArrayByte(Object array, long pos) { return UNSAFE.getByte(array, pos+16); }//unsafe访问数组需要往后偏移16位？？

    private final int DIGITS = 1;
    private final int CHARS = 2;
    private final int STRINGS = 3;
    private final int MINUS = 4;
    private final int SHARP = 5;
    private final int DIVISION = 6;
    private final byte DOT = 7;
    private final byte COMMA = 8;
    private final byte BACK_SLASH = 9;
    private final byte LEFT_PARENTHESES = 10;
    private final byte RIGHT_PARENTHESES = 11;
    private final byte SEMICOLON = 12;
    private final byte STAR = 13;
    private final byte EQUAL = 14;
    private final byte PLUS = 15;
    private final byte LESS = 16;
    private final byte GREATER = 17;
    private final byte AT = 18;
    private final byte COMMENTS = 19;

//    static final byte

    final byte[] charType = new byte[512];
    //final byte[] shrinkCharTbl = new byte[96];//为了压缩hash字符映射空间，再次进行转义
    HashArray hashArray = new HashArray();
    //TokenArray tokenArray = new TokenArray();
//    Unsafe UNSAFE;

    void init() {
        //// TODO: 2017/2/21 可能需要调整顺序进行优化
        IntStream.rangeClosed('0', '9').forEach(c -> charType[c<<1] = DIGITS);
        IntStream.rangeClosed('A', 'Z').forEach(c -> charType[c<<1] = CHARS);
        IntStream.rangeClosed('a', 'z').forEach(c -> charType[c<<1] = CHARS);
        charType['_'<<1] = CHARS;
        charType['$'<<1] = CHARS;
        charType['.'<<1] = DOT;
        charType[','<<1] = COMMA;
        //字符串
        charType['"'<<1] = STRINGS;
        charType['\''<<1] = STRINGS;
        charType['\\'<<1] = BACK_SLASH;
        //sql分隔
        charType['('<<1] = LEFT_PARENTHESES;
        charType[')'<<1] = RIGHT_PARENTHESES;
        charType[';'<<1] = SEMICOLON;
        //（可能的）注释和运算符
        charType['-'<<1] = MINUS;
        charType['/'<<1] = DIVISION;
        charType['#'<<1] = SHARP;
        charType['*'<<1] = STAR;
        charType['='<<1] = EQUAL;
        charType['+'<<1] = PLUS;
        charType['<'<<1] = LESS;
        charType['>'<<1] = GREATER;
        charType['@'<<1] = AT;

        charType[('$'<<1)+1] = 1;
        IntStream.rangeClosed('0', '9').forEach(c -> charType[(c<<1)+1] = (byte)(c-'0'+2));
        IntStream.rangeClosed('A', 'Z').forEach(c -> charType[(c<<1)+1] = (byte)(c-'A'+12));
        IntStream.rangeClosed('a', 'z').forEach(c -> charType[(c<<1)+1] = (byte)(c-'a'+12));
        charType[('_'<<1)+1] = 38;

        // 通过反射得到theUnsafe对应的Field对象
//        Field field = null;
//        try {
//            field =Unsafe.class.getDeclaredField("theUnsafe");
//            // 设置该Field为可访问
//            field.setAccessible(true);
//            // 通过Field得到该Field对应的具体对象，传入null是因为该Field为static的
//            UNSAFE = (Unsafe) field.get(null);
//        } catch (NoSuchFieldException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        }
    }

    int parseToken(byte[] sql, int pos, int sqlLength, byte c) {
        int cType;
        int start = pos;
        int size = 1;
        long hash = c = charType[(c<<1)+1];
        int type = 1315423911;
        type ^= (type<<5) + c + (type>>2);
        while (++pos < sqlLength && (((cType = charType[(c = sql[pos])<<1]) == 2) || cType == 1) ) {
            cType = charType[(c<<1)+1];
            hash = (hash*41)+cType;//别问我为什么是41
            type ^= (type<<5) + cType + (type>>2);
            size++;
        }
        hashArray.set(type, start, size, hash);
        return pos;
    }

    int parseString(byte[] sql, int pos, int sqlLength, int startSign) {
        int size = 1;
        int start = pos;
        int c;
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
        hashArray.set(STRINGS, start, size, 0L);
        return pos;
    }

    int parseDigits(byte[] sql, int pos, int sqlLength) {
        int start = pos;
        int size = 1;
        while (++pos<sqlLength && charType[sql[pos]<<1] == DIGITS) {
            size++;
        }
        hashArray.set(DIGITS, start, size);
        return pos;
    }

    int skipSingleLineComment(byte[] sql, int pos, int sqlLength) {
        while (++pos < sqlLength && sql[pos]!='\n');
        return pos;
    }

    int skipMultiLineComment(byte[] sql, int pos, int sqlLength, int pre) {
        int start = pos-1;
        int size=2;
        while (++pos < sqlLength) {
            if (pre == '*' && sql[pos] == '/' ) {
                size++;
            }
        }
        hashArray.set(COMMENTS, start, size);
        return pos;
    }

    void tokenize(byte[] sql) {
        int pos = 0;
        int sqlLength = sql.length;
        hashArray.init();
        byte c;
        byte cType;
        while (pos < sqlLength) {
            c = sql[pos];
            cType = charType[c<<1];

//            if (cType == CHARS) {
//                pos = parseToken(sql, pos, sqlLength, c);
//            } else if (cType == DIGITS) {
//                pos = parseDigits(sql, pos, sqlLength);
//            } else if (cType == MINUS) {
//                if (charType[sql[++pos]]!=MINUS) {
//                    hashArray.set(MINUS, pos++, (short)1);
//                } else {
//                    pos = skipSingleLineComment(sql, pos, sqlLength);
//                }
//            } else if (cType == STRINGS) {
//                pos = parseString(sql, ++pos, sqlLength, c);
//            } else if (cType == SHARP) {
//                pos = skipSingleLineComment(sql, pos, sqlLength);
//            } else if (cType == DIVISION) {
//                byte next = sql[++pos];
//                if (next == '*') {
//                    pos = skipMultiLineComment(sql, pos, sqlLength, next);
//                } if (next == '/') {
//                    pos = skipSingleLineComment(sql, pos, sqlLength);
//                } else {
//                    hashArray.set(charType[next<<1], pos++, (short)1);
//                }
//            } else if (cType != 0) {
//                hashArray.set(cType, pos++, (short)1);
//            } else {
//                pos++;
//            }

            switch (cType) {
                case 0:
                    pos++;
                    break;
                case CHARS:
                    pos = parseToken(sql, pos, sqlLength, c);
                    break;
                case DIGITS:
                    pos = parseDigits(sql, pos, sqlLength);
                    break;
                case STRINGS:
                    pos = parseString(sql, ++pos, sqlLength, c);
                    break;
                case MINUS:
                    if (sql[++pos]!='-') {
                        hashArray.set(MINUS, pos++, 1);
                    } else {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    }
                    break;
                case SHARP:
                    pos = skipSingleLineComment(sql, pos, sqlLength);
                    break;
                case DIVISION:
                    int next = sql[++pos];
                    if (next == '*') {
                        pos = skipMultiLineComment(sql, pos, sqlLength, next);
                    } if (next == '/') {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    } else {
                        hashArray.set(charType[next<<1], pos++, 1);
                    }
                    break;
                default:
                    hashArray.set(cType, pos++, 1);
            }
        }
    }

    void pickTableNames(int idx) {

    }
    /*
    * 用于进行第一遍处理，处理sql类型以及提取表名
     */
    public void firstParse(SQLContext context) {
        for(int i=0; i<hashArray.getCount(); i++) {
            switch (hashArray.getIntHash(i)) {
                case IntTokenHash.FROM:
                    if (hashArray.getHash(i) == TokenHash.FROM) {

                    }
                    break;
                case IntTokenHash.JOIN:
                    if (hashArray.getHash(i) == TokenHash.JOIN) {

                    }
                    break;
                case IntTokenHash.UPDATE:
                    if (hashArray.getHash(i) == TokenHash.UPDATE) {

                    }
                    break;
                case IntTokenHash.USE:
                    if (hashArray.getHash(i) == TokenHash.USE) {

                    }
                    break;
                case IntTokenHash.DELETE:
                    if (hashArray.getHash(i) == TokenHash.DELETE) {

                    }
                    break;
                case IntTokenHash.DROP:
                    if (hashArray.getHash(i) == TokenHash.DROP) {

                    }
                    break;
                case IntTokenHash.SELECT:
                    if (hashArray.getHash(i) == TokenHash.SELECT) {

                    }
                    break;
                case IntTokenHash.SHOW:
                    if (hashArray.getHash(i) == TokenHash.SHOW) {

                    }
                    break;
                case IntTokenHash.INSERT:
                    if (hashArray.getHash(i) == TokenHash.INSERT) {

                    }
                    break;
                case IntTokenHash.INTO:
                    if (hashArray.getHash(i) == TokenHash.INTO) {

                    }
                    break;
                default:
                    break;
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
