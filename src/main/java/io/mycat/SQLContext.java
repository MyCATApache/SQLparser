package io.mycat;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * by kaiz : 考虑将来可能要建立一个全局表用来管理/加速结果集/加速表名判定,最终形成一个用数组装载的简单ast
 * SQLContext 打算这样做：
 *      先返回String类型的表名和库名进行测试，
 *      再在内部维护统一的一个表名/库名表，
 *      之后通过一个统一的哈希值算法对表名/库名进行哈希，
 *      并根据哈希值在全局范围内作为表名/库名的唯一标识
 * SQLContext 主要提供如下功能：
 1. 当前SQL语句的hash值
 2. 当前SQL语句所用到的库名
 3. 当前SQL语句所用到的表名（需要和库名关联）
 4. 当前SQL语句的类型
 5. 当前SQL语句结果集要求（limit）
 */
public class SQLContext {
    private int[] tblResult;  //by kaiz : 记录格式：[{schema index(defaults 0), tbl name start pos, tbl name size}]
    private int[] schemaResult; //by kaiz : 记录格式：[{schema name pos, schema name size}]
    private byte tblCount;
    private short tblResultPos;
    private short tblResultSize;
    private byte schemaCount;
    private short schemaResultPos;
    private byte[] buffer;
    private int sqlHash;
    private byte sqlType;
    private int tblResultArraySize = 128;//todo : 测试期先写死，后期考虑从设置参数中读取 by kaiz
    private int schemaResultArraySize = 64;//todo : 测试期先写死，后期考虑从设置参数中读取 by kaiz

    //DDL
    public static final byte CREATE_SQL = 1;
    public static final byte ALTER_SQL = 2;
    public static final byte DROP_SQL = 3;
    public static final byte TRUNCATE_SQL = 4;
    public static final byte COMMENT_SQL = 5;
    public static final byte RENAME_SQL = 6;
    public static final byte USE_SQL = 7;
    public static final byte SHOW_SQL = 8;

    //DML
    public static final byte SELECT_SQL = 10;
    public static final byte UPDATE_SQL = 11;
    public static final byte DELETE_SQL = 12;
    public static final byte INSERT_SQL = 13;
    public static final byte REPLACE_SQL = 14;
    public static final byte CALL_SQL = 15;
    public static final byte EXPLAIN_SQL = 16;
    public static final byte LOCK_SQL = 17;

    //DCL
    public static final byte GRANT_SQL = 20;
    public static final byte REVOKE_SQL = 21;

    //TCL
    public static final byte SAVEPOINT_SQL = 10;
    public static final byte ROLLBACK_SQL = 11;
    public static final byte SET_TRANSACTION_SQL = 12;

    public SQLContext() {
        tblResult = new int[tblResultArraySize];
        schemaResult = new int[schemaResultArraySize];
    }

    public void setCurBuffer(byte[] curBuffer) {
        buffer = curBuffer;
        tblCount = 0;
        schemaCount = 0;
        tblResultPos = 0;
        schemaResultPos = 2;
        Arrays.fill(tblResult, 0);
        Arrays.fill(schemaResult, 0);
        sqlHash = 0;
        sqlType = 0;
    }

    public void setTblNameStart(int pos) {
        tblCount++;
        tblResultPos++; //by kaiz : 跳过第一个schema，因为有可能之前已经设置过了
        tblResult[tblResultPos++] = pos;
        tblResult[tblResultPos] = 1;
    }

    public boolean isTblNameEnd() {
        return tblResultPos%3 == 0;
    }

    public void setTblNameSize(int size) {
        tblResult[tblResultPos++] = size;
    }

    public void pushSchemaName() {
        schemaCount++;
        schemaResult[schemaResultPos++]=tblResult[tblResultPos-2];
        schemaResult[schemaResultPos++]=tblResult[tblResultPos-1];
        tblCount--;
        tblResult[--tblResultPos] = 0;//by kaiz
        tblResult[--tblResultPos] = 0;
        tblResult[--tblResultPos] = schemaCount;
    }

    public int getTableCount() { return tblCount; }

    public int getSchemaCount() { return schemaCount; }

    public String getSchemaName(int tblIdx) {
        int schemaIdx = tblResult[3*tblIdx];
        if (schemaIdx == 0)
            return "default";
        else {
            int schemaResultOffset = schemaIdx<<1;
            int offset = schemaResult[schemaResultOffset];
            int size = schemaResult[schemaResultOffset+1];
            return new String(buffer, offset, size);
        }
    }

    //todo : 测试期返回String，将来应该要返回hashcode
    public String getTableName(int i) {
        int offset = tblResult[3*i+1];
        int size = tblResult[3*i+2];
        return new String(buffer, offset, size);
        //return builder.append((char[])buffer, offset, size).toString();
    }

    public void setSQLHash(int sqlHash) { this.sqlHash = sqlHash; }

    public int getSqlHash() { return this.sqlHash; }

    public void setSQLType(byte sqlType) {
        if (this.sqlType == 0)
            this.sqlType = sqlType;
    }

    public byte getSQLType() { return this.sqlType; }

}
