package io.mycat;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

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
 * 2017/3/8
 * 还可以进行如下优化：
 * 1. 部分变量可以替换成常量（是否需要预编译）
 * 2. 使用堆外unsafe数组
 * 3. SQLContext还需要优化成映射到hashArray的模式，也可以考虑只用一个数组，同时存储hashArray、charType、token和解析结果（估计也没人看得懂了）
 *
 */


public class NewSQLParser {
    SQLContext context;
    SQLReader reader;

    class HashArray {
        long[] hashArray = new long[4096];
        int pos = 0;

        void init() {
            while(pos>=0) {
                hashArray[pos--] = 0;
            }
            pos = 0;
        }
        void set(int type, int start, int size) { hashArray[pos++] = (long)type << 32 | size << 16 | start; pos++; }
        void set(int type, int start, int size, long hash) { hashArray[pos++] = (long)type << 32 | size << 16 | start; hashArray[pos++] = hash; }
        int getPos(int idx) { return (((int)hashArray[idx<<1]) & 0xFFFF); }
        int getSize(int idx) { return (((int)hashArray[idx<<1]&0xFFFF0000) >>> 16); }
        int getType(int idx) { return (int)((hashArray[idx<<1]&0xFFFFFFFF00000000L)>>>32); }
        void setType(int idx, int type) { hashArray[idx<<1] = (hashArray[idx<<1] & 0xFFFFFFFFL) | ((long)type << 32); }
        long getHash(int idx) { return hashArray[(idx<<1)+1]; }
        int getIntHash(int idx) {
            long value = hashArray[idx << 1];
            return (int)(value >>> 16);
        }
        int getCount() {return pos>>1;}
    }

    private final int DIGITS = 1;
    private final int CHARS = 2;
    private final int STRINGS = 3;
    private final int MINUS = 4;
    private final int SHARP = 5;
    private final int DIVISION = 6;
    private final byte AT = 7;
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
    private final byte DOT = 18;
    private final byte COMMENTS = 19;

    byte[] sql;
    final byte[] charType = new byte[512];
    HashArray hashArray = new HashArray();

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
        return ++pos;
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

    int skipMultiLineComment(byte[] sql, int pos, int sqlLength, byte pre) {
        //int start = pos-2;
        //int size=2;
        byte cur = sql[pos];
        while (pos < sqlLength) {
            if (pre == '*' && cur == '/' ) {
                return ++pos;
            }
            pos++;
            pre = cur;
            cur = sql[pos];
        }
        //hashArray.set(COMMENTS, start, size);
        return pos;
    }

    void tokenize(byte[] sql) {
        int pos = 0;
        int sqlLength = sql.length;
        this.sql = sql;
        hashArray.init();
        byte c;
        byte cType;
        byte next;
        while (pos < sqlLength) {
            c = sql[pos];
            cType = charType[c<<1];

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
                    pos = parseString(sql, pos, sqlLength, c);
                    break;
                case MINUS:
                    if (sql[++pos]!='-') {
                        hashArray.set(MINUS, pos-1, 1);
                    } else {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    }
                    break;
                case SHARP:
                    pos = skipSingleLineComment(sql, pos, sqlLength);
                    break;
                case DIVISION:
                    next = sql[++pos];
                    if (next == '*') {
                        next = sql[++pos];
                        if (next == '#'||next == '!') {
                            //处理mycat注解
                        } //还有 /*balance*/ 注解
                        pos = skipMultiLineComment(sql, ++pos, sqlLength, next);
                    } else if (next == '/') {
                        pos = skipSingleLineComment(sql, pos, sqlLength);
                    } else {
                        hashArray.set(charType[next<<1], pos++, 1);
                    }
                    break;
                case AT:
                    next = sql[++pos];
                    if (next == '@') {
                        //parse system infomation
                    }
                default:
                    hashArray.set(cType, pos++, 1);
            }
        }
    }

    boolean isAlias(int pos, int type) { //需要优化成数组判断
        switch (type) {
            case IntTokenHash.WHERE:
                if (hashArray.getHash(pos) == TokenHash.WHERE)
                    return false;
                else
                    return true;
            case IntTokenHash.GROUP:
                if (hashArray.getHash(pos) == TokenHash.GROUP)
                    return false;
                else
                    return true;
            case IntTokenHash.ORDER:
                if (hashArray.getHash(pos) == TokenHash.ORDER)
                    return false;
                else
                    return true;
            case IntTokenHash.LIMIT:
                if (hashArray.getHash(pos) == TokenHash.LIMIT)
                    return false;
                else
                    return true;
            case IntTokenHash.JOIN:
                if (hashArray.getHash(pos) == TokenHash.JOIN)
                    return false;
                else
                    return true;
            case IntTokenHash.LEFT:
                if (hashArray.getHash(pos) == TokenHash.LEFT)
                    return false;
                else
                    return true;
            case IntTokenHash.RIGHT:
                if (hashArray.getHash(pos) == TokenHash.RIGHT)
                    return false;
                else
                    return true;
            case IntTokenHash.FOR:
                if (hashArray.getHash(pos) == TokenHash.FOR)
                    return false;
                else
                    return true;
            case IntTokenHash.LOCK:
                if (hashArray.getHash(pos) == TokenHash.LOCK)
                    return false;
                else
                    return true;
            case IntTokenHash.ON:
                if (hashArray.getHash(pos) == TokenHash.ON)
                    return false;
                else
                    return true;
            default:
                return true;
        }
    }

    int pickTableNames(int pos, final int arrayCount, SQLContext context) {
        int type;
        long hash = hashArray.getHash(pos);
        if (hash != 0) {
            context.setTblNameStart(hashArray.getPos(pos));// TODO: 2017/3/10 可以优化成一个接口
            context.setTblNameSize(hashArray.getSize(pos));
            pos++;
            while (pos < arrayCount) {
                type = hashArray.getType(pos);
                if (type == DOT) {
                    ++pos;
                    context.pushSchemaName();
                    context.setTblNameStart(hashArray.getPos(pos));// TODO: 2017/3/10 可以优化成一个接口
                    context.setTblNameSize(hashArray.getSize(pos));
                    ++pos;
                } else if (type == SEMICOLON || type == RIGHT_PARENTHESES) {
                    return ++pos;
                } else if (type == COMMA) {
                    return pickTableNames(++pos, arrayCount, context);
                } else if ((type = hashArray.getIntHash(pos)) == IntTokenHash.AS) {
                    pos += 2;// TODO: 2017/3/10  二阶段解析需要别名，需要把别名存储下来
                } else if (type == COMMENTS) {
                    pos++;
                } else if (isAlias(pos, type)) {
                    pos++;// TODO: 2017/3/10  二阶段解析需要别名，需要把别名存储下来
                } else
                    return pos;
            }
            return pos;
        } else {//判断 ,( 这样的情况
            return ++pos;
        }
    }

    int pickNumber(int pos) {
        int value = 0;
        int start = hashArray.getPos(pos);
        int end = start + hashArray.getSize(pos);
        for (int i=start; i<end; i++) {
            value = (value*10)+(sql[i]-'0');
        }
        return value;
    }

    int pickLimits(int pos, final int arrayCount, SQLContext context) {
        int minus = 1;
        if (hashArray.getType(pos) == DIGITS) {
            context.setLimit();
            context.setLimitCount(pickNumber(pos));
            if (++pos < arrayCount && hashArray.getType(pos) == COMMA) {
                context.pushLimitStart();
                if (++pos < arrayCount) {
                    if (hashArray.getType(pos) == MINUS) {
                        minus = -1;
                        ++pos;
                    }
                    if (hashArray.getType(pos) == DIGITS) {
                        //// TODO: 2017/3/11 需要完善处理数字部分逻辑
                        context.setLimitCount(pickNumber(pos)*minus);
                    }
                }
            } else if (hashArray.getHash(pos) == TokenHash.OFFSET) {
                context.setLimitStart(pickNumber(++pos));
            }
        }
        return ++pos;
    }

    int pickInsert(int pos, final int arrayCount, SQLContext context) {
        int intHash;
        long hash;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            if (intHash == IntTokenHash.INTO && hash == TokenHash.INTO) {
                return pickTableNames(++pos, arrayCount, context);
            } else if (intHash == IntTokenHash.DELAYED && hash == TokenHash.DELAYED) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.LOW_PRIORITY && hash == TokenHash.LOW_PRIORITY) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.HIGH_PRIORITY && hash == TokenHash.HIGH_PRIORITY) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.IGNORE && hash == TokenHash.IGNORE) {
                pos++;
                continue;
            } else {
                return pickTableNames(pos, arrayCount, context);
            }

        }
        return pos;
    }

    int pickTableToken(int pos, final int arrayCount, SQLContext context) {
        int intHash;
        long hash;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            if (intHash == IntTokenHash.IF && hash == TokenHash.IF) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.NOT && hash == TokenHash.NOT) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.EXISTS && hash == TokenHash.EXISTS) {
                pos++;
                continue;
            } else {
                return pickTableNames(pos, arrayCount, context);
            }
        }
        return pos;
    }

    int pickUpdate(int pos, final int arrayCount, SQLContext context) {
        int intHash;
        long hash;
        while (pos < arrayCount) {
            intHash = hashArray.getIntHash(pos);
            hash = hashArray.getHash(pos);
            if (intHash == IntTokenHash.LOW_PRIORITY && hash == TokenHash.LOW_PRIORITY) {
                pos++;
                continue;
            } else if (intHash == IntTokenHash.IGNORE && hash == TokenHash.IGNORE) {
                pos++;
                continue;
            } else {
                return pickTableNames(pos, arrayCount, context);
            }

        }
        return pos;
    }


    /*
    * 用于进行第一遍处理，处理sql类型以及提取表名
     */
    public void firstParse(SQLContext context) {
        final int arrayCount = hashArray.getCount();
        int pos = 0;
        while(pos<arrayCount) {
            switch (hashArray.getIntHash(pos)) {
                case IntTokenHash.FROM:
                    if (hashArray.getHash(pos) == TokenHash.FROM) {
                        pos = pickTableNames(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.INTO:
                    if (hashArray.getHash(pos) == TokenHash.INTO) {
                        pos = pickTableNames(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.TABLE:
                    if (hashArray.getHash(pos) == TokenHash.TABLE) {
                        pos = pickTableToken(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.JOIN:
                    if (hashArray.getHash(pos) == TokenHash.JOIN) {
                        pos = pickTableNames(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.UPDATE:
                    if (hashArray.getHash(pos) == TokenHash.UPDATE) {
                        context.setSQLType(SQLContext.UPDATE_SQL);
                        pos = pickUpdate(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.USE:
                    if (hashArray.getHash(pos) == TokenHash.USE) {
                        context.setSQLType(SQLContext.USE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.DELETE:
                    if (hashArray.getHash(pos) == TokenHash.DELETE) {
                        context.setSQLType(SQLContext.DELETE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.DROP:
                    if (hashArray.getHash(pos) == TokenHash.DROP) {
                        context.setSQLType(SQLContext.DROP_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.SELECT:
                    if (hashArray.getHash(pos) == TokenHash.SELECT) {
                        context.setSQLType(SQLContext.SELECT_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.SHOW:
                    if (hashArray.getHash(pos) == TokenHash.SHOW) {
                        context.setSQLType(SQLContext.SHOW_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.INSERT:
                    if (hashArray.getHash(pos) == TokenHash.INSERT) {
                        context.setSQLType(SQLContext.INSERT_SQL);
                        pos = pickInsert(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.LIMIT:
                    if (hashArray.getHash(pos) == TokenHash.LIMIT) {
                        pos = pickLimits(++pos, arrayCount, context);
                    }
                    break;
                case IntTokenHash.TRUNCATE:
                    if (hashArray.getHash(pos) == TokenHash.TRUNCATE) {
                        context.setSQLType(SQLContext.TRUNCATE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.ALTER:
                    if (hashArray.getHash(pos) == TokenHash.ALTER) {
                        context.setSQLType(SQLContext.ALTER_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.CREATE:
                    if (hashArray.getHash(pos) == TokenHash.CREATE) {
                        context.setSQLType(SQLContext.CREATE_SQL);
                        pos++;
                    }
                    break;
                case IntTokenHash.REPLACE:
                    if (hashArray.getHash(pos) == TokenHash.REPLACE) {
                        context.setSQLType(SQLContext.REPLACE_SQL);
                        pos++;
                    }
                    break;

                default:
                    pos++;
                    break;
            }
        }

    }

    /*
    * 计划用于第二遍解析，处理分片表分片条件
    */
    public void secondParse() {

    }

    public void parse(byte[] src, SQLContext context) {
        context.setCurBuffer(src);
        tokenize(src);
        firstParse(context);
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
        SQLContext context = new SQLContext();
        parser.init();
//        byte[] src = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));".getBytes(StandardCharsets.UTF_8);//20个token
//        byte[] src = "INSERT `schema`.`tbl_A` (`name`) VALUES ('kaiz');".getBytes(StandardCharsets.UTF_8);
//        byte[] src = ("select * from tbl_A, -- 单行注释\n" +
//                "tbl_B b, #另一种单行注释\n" +
//                "/*\n" +  //69
//                "tbl_C\n" + //79
//                "*/ tbl_D d;").getBytes(StandardCharsets.UTF_8);
//        byte[] src = sql3.getBytes(StandardCharsets.UTF_8);
        byte[] src = "SELECT * FROM table LIMIT 95,-1".getBytes(StandardCharsets.UTF_8);

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
        parser.parse(src, context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i) + '.' + context.getTableName(i)));
        //System.out.print("token count : "+parser.hashArray.getCount());
    }

    static String sql3 = "SELECT  'product' as 'P_TYPE' ,\n" +
            "\t \t\tp.XINSHOUBIAO,\n" +
            "\t\t\t0 AS TRANSFER_ID,\n" +
            "\t\t\tp.PRODUCT_ID ,\n" +
            "\t\t\tp.PRODUCT_NAME,\n" +
            "\t\t\tp.PRODUCT_CODE,\n" +
            "\t\t\tROUND(p.APPLY_INTEREST,4) AS APPLY_INTEREST,\n" +
            "\t\t\tp.BORROW_AMOUNT,\n" +
            "\t\t\tCASE  WHEN p.FangKuanDate IS NULL THEN\n" +
            "\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\tp.APPLY_ENDDATE,\n" +
            "\t\t\t\t\tDATE_ADD(\n" +
            "\t\t\t\t\t\tp.RAISE_END_TIME,\n" +
            "\t\t\t\t\t\tINTERVAL 1 DAY\n" +
            "\t\t\t\t\t)\n" +
            "\t\t\t\t) +1\n" +
            "\t\t\tELSE\n" +
            "\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\tp.APPLY_ENDDATE,\n" +
            "\t\t\t\t\tDATE_ADD(p.FangKuanDate, INTERVAL 1 DAY)\n" +
            "\t\t\t\t) +1\n" +
            "\t\t\tEND\t AS APPLY_ENDDAY,\n" +
            "\t\t\t'' AS APPLY_ENDDATE,\n" +
            "\t\t\tp.BORROW_ENDDAY,\n" +
            "\t\t\t0 AS TRANSFER_HONGLI,\n" +
            "\t\t\tp.BORROW_MONTH_TYPE,\n" +
            "\t\t\tIFNULL(p.INVEST_SCHEDUL,0) AS INVEST_SCHEDUL,\n" +
            "\t\t\tDATE_FORMAT(\n" +
            "\t\t\t\tp.Product_pub_date,\n" +
            "\t\t\t\t'%Y-%m-%d %H:%i:%s'\n" +
            "\t\t\t) AS Product_pub_date,\n" +
            " \t\t\td.DIZHIYA_TYPE_NAME,\n" +
            "\t\t\tp.PRODUCT_TYPE_ID,\n" +
            "\t\t\tp.PRODUCT_STATE,\n" +
            "\t\t\tp.PRODUCT_LIMIT_TYPE_ID,\n" +
            "\t\t\tp.PAYBACK_TYPE,\n" +
            "\t\t\tp.TARGET_TYPE_ID,\n" +
            "\t\t\tp.COUPONS_TYPE,\n" +
            "      0 AS TRANSFER_TIME,\n" +
            "      P.MANBIAODATE AS  MANBIAODATE\n" +
            "\t\tFROM\n" +
            "\t\t\tTProduct p\n" +
            "\t\tJOIN TJieKuanApply j ON p.APPLY_NO = j.APPLY_NO\n" +
            "\t\tJOIN TDiZhiYaType d ON d.DIZHIYA_TYPE = j.DIZHIYA_TYPE\n" +
            "\t\tJOIN (\n" +
            "\t\t\tSELECT\n" +
            "\t\t\n" +
            "\t\t\t\tPRODUCT_ID,\n" +
            "\t\t\t\tCASE\n" +
            "\t\t\tWHEN APPLY_ENDDATE IS NOT NULL THEN\n" +
            "\t\t\t\tCASE  WHEN FangKuanDate IS NULL THEN\n" +
            "\t\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\t\tAPPLY_ENDDATE,\n" +
            "\t\t\t\t\t\tDATE_ADD(\n" +
            "\t\t\t\t\t\t\tRAISE_END_TIME,\n" +
            "\t\t\t\t\t\t\tINTERVAL 1 DAY\n" +
            "\t\t\t\t\t\t)\n" +
            "\t\t\t\t\t) +1\n" +
            "\t\t\t\tELSE\n" +
            "\t\t\t\t\tDATEDIFF(\n" +
            "\t\t\t\t\t\tAPPLY_ENDDATE,\n" +
            "\t\t\t\t\t\tDATE_ADD(FangKuanDate, INTERVAL 1 DAY)\n" +
            "\t\t\t\t\t) +1\n" +
            "\t\t\t\tEND\n" +
            "\t\t\tWHEN BORROW_MONTH_TYPE = 1 THEN\n" +
            "\t\t\t\tBORROW_ENDDAY\n" +
            "\t\t\tWHEN BORROW_MONTH_TYPE = 2 THEN\n" +
            "\t\t\t\tBORROW_ENDDAY * 30\n" +
            "\t\t\tELSE\n" +
            "\t\t\t\tBORROW_ENDDAY\n" +
            "\t\t\tEND AS DAYS\n" +
            "\t\t\tFROM\n" +
            "\t\t\t\tTProduct\n" +
            "\t\t\t) m ON p.PRODUCT_ID = m.PRODUCT_ID\n" +
            "\t\tWHERE\n" +
            "\t\t     1 = 1\n" +
            "\t\t     AND p.PRODUCT_STATE IN(4,5,8) \n" +
            "\t\t     AND (p.PRODUCT_ID) NOT IN (\n" +
            "\t\t\t SELECT PRODUCT_ID FROM TProduct WHERE PRODUCT_STATE = 4 ";
}
