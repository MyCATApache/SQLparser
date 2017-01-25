package io.mycat;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.IntStream;

/**
 * Created by Kaiz on 2017/1/22.
 */
public class SQLParserTest extends TestCase {
    SQLParser parser;
    SQLContext context;

    @Before
    protected void setUp() throws Exception {
        parser = new SQLParser();
        context = new SQLContext();
    }

    @Test
    public void testNormalSelect() throws Exception {
        String t = "SELECT * FROM a";
        parser.parse(t.getBytes(), context);
        assertEquals(1, context.getTableCount());
    }

    @Test
    public void testMultiTableSelect() throws Exception {
        String t = "SELECT a.*, b.* FROM tbl_A a , tbl_B b;";
        parser.parse(t.getBytes(), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i)+'.'+context.getTableName(i)));
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testJoinSelect() {
        String t = "SELECT a.*, b.* FROM tbl_A a left join tbl_B b on b.id=a.id;";
        parser.parse(t.getBytes(), context);
        assertEquals(2, context.getTableCount());
    }

    @Test
    public void testNestSelect() throws Exception {
        String sql = "SELECT a fROm ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));";
        parser.parse(sql.getBytes(), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i)+'.'+context.getTableName(i)));
        assertEquals(5, context.getTableCount());
        assertEquals("ab", context.getTableName(0));
        assertEquals("ff", context.getTableName(1));
        assertEquals("tbl_bb", context.getTableName(2));
        assertEquals("ccc", context.getTableName(3));
        assertEquals("dddd", context.getTableName(4));
        assertEquals("schema_bb", context.getSchemaName(2));
    }

    @Test
    public void testCase01() throws Exception {
        String sql = "select sum(convert(borrow_principal/100, decimal(18,2))) '借款本金'\n" +
                "    from s_user_borrow_record_status\n" +
                "    where 1=1\n" +
                "    and create_at >= '2017-01-04 00:00:00'\n" +
                "    and create_at <= '2017-01-04 23:59:59';";
        parser.parse(sql.getBytes(), context);
        assertEquals("s_user_borrow_record_status", context.getTableName(0));
    }

    @Test
    public void testNormalUpdate() throws Exception {
        String sql = "UPDATE tbl_A set name='kaiz' where name='nobody';";
        parser.parse(sql.getBytes(), context);
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalDelete() throws Exception {
        String sql = "DELETE FROM tbl_A WHERE name='nobody';";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.DELETE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalInsert() throws Exception {
        String sql = "INSERT INTO tbl_A (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalInsert2() throws Exception {
        String sql = "INSERT `schema`.`tbl_A` (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testIgnoreInsert() throws Exception {
        String sql = "INSERT IGNORE tbl_A (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.INSERT_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalReplace() throws Exception {
        String sql = "Replace into tbl_A (`name`) VALUES ('kaiz');";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.REPLACE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalAlter() throws Exception {
        String sql = "ALTER TABLE tbl_A ADD name VARCHAR(15) NULL;";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.ALTER_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testDropAlter() throws Exception {
        String sql = "ALTER TABLE tbl_A DROP name VARCHAR(15) NULL;";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.ALTER_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalDrop() throws Exception {
        String sql = "DROP TABLE IF EXISTS tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.DROP_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalCreate() throws Exception {
        String sql = "CREATE TABLE IF NOT EXISTS tbl_A ( Id INT NOT NULL UNIQUE PRIMARY KEY, name VARCHAR(20) NOT NULL,;";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.CREATE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }
}
