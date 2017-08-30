package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.NewSQLContext2;
import io.mycat.mycat2.sqlparser.byteArrayInterface.NewSQLParser2;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

/**
 * Created by cjw on 2017/1/22.
 */
public class TCLSQLParserTest extends TestCase {
    //    SQLParser parser;
    NewSQLParser2 parser;
    NewSQLContext2 context;
    private static final Logger LOGGER = LoggerFactory.getLogger(TCLSQLParserTest.class);

    private void test(String t) {
        LOGGER.info(t);
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
    }

    @Before
    protected void setUp() throws Exception {
        parser = new NewSQLParser2();
        context = new NewSQLContext2();
        //parser.init();
        MatchMethodGenerator.initShrinkCharTbl();
    }

    @Test
    public void testStartTransactionWithConsistentSnapshot() throws Exception {
        String t = "START TRANSACTION WITH CONSISTENT SNAPSHOT;";
        test(t);
    }

    @Test
    public void testStartTransaction() throws Exception {
        String t = "START TRANSACTION;";
        test(t);
    }

    @Test
    public void testStartTransactionReadWrite() throws Exception {
        String t = "START TRANSACTION READ WRITE;";
        test(t);
    }

    @Test
    public void testStartTransactionReadOnly() throws Exception {
        String t = "START TRANSACTION READ ONLY;";
        test(t);
    }

    @Test
    public void testStartTransactionTwoParameters() throws Exception {
        String t = "START TRANSACTION READ ONLY,READ WRITE;";
        test(t);
    }

    @Test
    public void testStartTransactionThreeParameters() throws Exception {
        String t = "START TRANSACTION WITH CONSISTENT SNAPSHOT,READ WRITE,READ ONLY;";
        test(t);
    }

    @Test
    public void testSetAutocommit() throws Exception {
        String t = "SET AUTOCOMMIT = 1;";
        test(t);
    }

    @Test
    public void testBegin() throws Exception {
        String t = "BEGIN;";
        test(t);
    }

    @Test
    public void testBeginWork() throws Exception {
        String t = "BEGIN WORK;";
        test(t);
    }

    @Test
    public void testCommit() throws Exception {
        String t = "COMMIT WORK AND NO CHAIN NO RELEASE";
        test(t);
    }

    @Test
    public void testCommit1() throws Exception {
        String t = "COMMIT WORK AND NO CHAIN  RELEASE;";
        test(t);
        ;
    }

    @Test
    public void testCommit2() throws Exception {
        String t = "COMMIT WORK AND  CHAIN  RELEASE;";
        test(t);
    }

    @Test
    public void testCommit3() throws Exception {
        String t = "COMMIT WORK AND CHAIN;";
        test(t);
    }

    @Test
    public void testCommit4() throws Exception {
        String t = "COMMIT WORK;";
        test(t);
    }

    @Test
    public void testCommit5() throws Exception {
        String t = "COMMIT;";
        test(t);
    }

    @Test
    public void testCommit6() throws Exception {
        String t = "COMMIT NO RELEASE;";
        test(t);
    }

    @Test
    public void testCommit7() throws Exception {
        String t = "COMMIT RELEASE;";
        test(t);
    }

    /**
     * for testCommit3
     **/
    @Test
    public void testCommit8() throws Exception {
        String t = "COMMIT WORK AND NO CHAIN;";
        test(t);
    }

    @Test
    public void testRollback() throws Exception {
        String t = "ROLLBACK WORK AND NO CHAIN NO RELEASE";
        test(t);
    }

}
