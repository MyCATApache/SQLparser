package io.mycat;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
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
        String t = "SELECT a.*, b.* FROM tbl_A a, tbl_B b , tbl_C c;";
        parser.parse(t.getBytes(), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i)+'.'+context.getTableName(i)));
        assertEquals(3, context.getTableCount());
    }

    @Test
    public void testJoinSelect() {
        String t = "SELECT a.*, b.* FROM tbl_A as a left join tbl_B b on b.id=a.id;";
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
        String sql = "CREATE TABLE IF NOT EXISTS tbl_A ( Id INT NOT NULL UNIQUE PRIMARY KEY, name VARCHAR(20) NOT NULL;";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.CREATE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
    }

    @Test
    public void testNormalTruncate() throws Exception {
        String sql = "Truncate TABLE IF EXISTS tbl_A;";
        parser.parse(sql.getBytes(), context);
        assertEquals(SQLContext.TRUNCATE_SQL, context.getSQLType());
        assertEquals("tbl_A", context.getTableName(0));
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
    public void testCase02() throws Exception {
        parser.parse(sql1.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i)+'.'+context.getTableName(i)));
        assertEquals(8, context.getTableCount());
    }

    @Test
    public void testCase03() throws Exception {
        parser.parse(sql2.getBytes(StandardCharsets.UTF_8), context);
        IntStream.range(0, context.getTableCount()).forEach(i -> System.out.println(context.getSchemaName(i)+'.'+context.getTableName(i)));
        assertEquals(17, context.getTableCount());
    }

//    @Test
//    public void testNormalTruncate() throws Exception {
//        String sql = "Truncate TABLE IF EXISTS tbl_A;";
//        parser.parse(sql.getBytes(), context);
//        assertEquals(SQLContext.TRUNCATE_SQL, context.getSQLType());
//        assertEquals("tbl_A", context.getTableName(0));
//    }
private static final String sql1 = "select t3.*,ztd3.TypeDetailName as UseStateName\n" +
        "from\n" +
        "( \n" +
        " select t4.*,ztd4.TypeDetailName as AssistantUnitName\n" +
        " from\n" +
        " (\n" +
        "  select t2.*,ztd2.TypeDetailName as UnitName \n" +
        "  from\n" +
        "  (\n" +
        "   select t1.*,ztd1.TypeDetailName as MaterielAttributeName \n" +
        "   from \n" +
        "   (\n" +
        "    select m.*,r.RoutingName,u.username,mc.MoldClassName\n" +
        "    from dbo.D_Materiel as m\n" +
        "    left join dbo.D_Routing as r\n" +
        "    on m.RoutingID=r.RoutingID\n" +
        "    left join dbo.D_MoldClass as mc\n" +
        "    on m.MoldClassID=mc.MoldClassID\n" +
        "    left join dbo.D_User as u\n" +
        "    on u.UserId=m.AddUserID\n" +
        "   )as t1\n" +
        "   left join dbo.D_Type_Detail as ztd1 \n" +
        "   on t1.MaterielAttributeID=ztd1.TypeDetailID\n" +
        "  )as t2\n" +
        "  left join dbo.D_Type_Detail as ztd2 \n" +
        "  on t2.UnitID=ztd2.TypeDetailID\n" +
        " ) as t4\n" +
        " left join dbo.D_Type_Detail as ztd4 \n" +
        " on t4.AssistantUnitID=ztd4.TypeDetailID\n" +
        ")as t3\n" +
        "left join dbo.D_Type_Detail as ztd3 \n" +
        "on t3.UseState=ztd3.TypeDetailID";


    private static final String sql2 = "Select d.Fabric_No,\n" +
            "       f.MachineName,\n" +
            "       f.RawNo,\n" +
            "       f.OldRawNo,\n" +
            "       f.RawName,\n" +
            "       f.StructCode,\n" +
            "       p.WorkClass,\n" +
            "       d.DefectType,\n" +
            "       d.DefectName,\n" +
            "       f.InspectResult,\n" +
            "       Convert(Char(10), InspectDate, 20) As InspectDate,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          Convert(Decimal(28, 2),\n" +
            "                  (d.DefectEnd - d.DefectStart + 1) /\n" +
            "                  dbo.f_JT_CalcMinValue(LPair, RPair) * Allow_Qty)\n" +
            "         Else\n" +
            "          (d.DefectEnd - d.DefectStart + 1)\n" +
            "       End) As MLength,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          ISNULL((Select SUM(DefectEnd)\n" +
            "                   From FIInspectFabricDefects s\n" +
            "                  Where DefectStart >= d.DefectStart\n" +
            "                    And DefectStart <= d.DefectEnd\n" +
            "                    And Fabric_No = d.Fabric_No\n" +
            "                    And RecType = '疵点'),\n" +
            "                 0.00)\n" +
            "         Else\n" +
            "          ISNULL((Select SUM(DefectEnd - DefectStart + 1)\n" +
            "                   From FIInspectFabricDefects s\n" +
            "                  Where DefectStart >= d.DefectStart\n" +
            "                    And DefectStart <= d.DefectEnd\n" +
            "                    And DefectEnd >= d.DefectStart\n" +
            "                    And DefectEnd <= d.DefectEnd\n" +
            "                    And Fabric_No = d.Fabric_No\n" +
            "                    And RecType = '疵点'),\n" +
            "                 0.00)\n" +
            "       End) As DefectNum,\n" +
            "       Convert(Decimal(28, 2),\n" +
            "               (d.DefectEnd - d.DefectStart + 1.0) / (Case\n" +
            "                 When f.StructCode = 'JT' Then\n" +
            "                  dbo.f_JT_CalcMinValue(LPair, RPair)\n" +
            "                 Else\n" +
            "                  f.Allow_Qty\n" +
            "               End) * f.Allow_Wt) As MWeight,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "         Else\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "       End) As OneDefectName,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "         Else\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 1)\n" +
            "       End) As OneDefect,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "         Else\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "       End) As TwoDefectName,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "         Else\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 2)\n" +
            "       End) As TwoDefect,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "         Else\n" +
            "          (Select B.DefectName\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "       End) As ThreeDefectName,\n" +
            "       (Case\n" +
            "         When f.StructCode = 'JT' Then\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk * DefectEnd) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "         Else\n" +
            "          (Select B.DefectNum\n" +
            "             From (Select A.*,\n" +
            "                          ROW_NUMBER() Over(Order By DefectNum Desc) As RecNo\n" +
            "                     From (Select SUM(DefectBulk *\n" +
            "                                      (DefectEnd - DefectStart + 1)) As DefectNum,\n" +
            "                                  DefectName,\n" +
            "                                  DefectType\n" +
            "                             From FIInspectFabricDefects s\n" +
            "                            Where s.RecType = '疵点'\n" +
            "                              And s.Fabric_No = d.Fabric_No\n" +
            "                              And s.DefectStart >= d.DefectStart\n" +
            "                              And s.DefectStart <= d.DefectEnd\n" +
            "                              And s.DefectEnd >= d.DefectStart\n" +
            "                              And s.DefectEnd <= d.DefectEnd\n" +
            "                            Group By DefectType, DefectName) A) B\n" +
            "            Where B.RecNo = 3)\n" +
            "       End) As ThreeDefect\n" +
            "  From FIInspectFabric f, FIInspectFabricDefects d, t_ORC_StanPersonSet P\n" +
            " Where d.RecType = '产量'\n" +
            "   And d.DefectType = p.PNo\n" +
            "   And f.Fabric_NO = d.Fabric_No\n" +
            " Order By d.Fabric_No, p.WorkClas\n";

}
