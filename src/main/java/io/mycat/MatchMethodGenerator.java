package io.mycat;

import com.alibaba.druid.sql.parser.Token;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by Administrator on 2017/2/13 0013.
 */
public class MatchMethodGenerator {

    static Map<String, Token> map = new HashMap<String, Token>();

    static {

        map.put("ALL", Token.ALL);
        map.put("ALTER", Token.ALTER);
        map.put("AND", Token.AND);
        map.put("ANY", Token.ANY);
        map.put("AS", Token.AS);
        map.put("ENABLE", Token.ENABLE);
        map.put("DISABLE", Token.DISABLE);
        map.put("ASC", Token.ASC);
        map.put("BETWEEN", Token.BETWEEN);
        map.put("BY", Token.BY);
        map.put("CASE", Token.CASE);
        map.put("CAST", Token.CAST);
        map.put("CHECK", Token.CHECK);
        map.put("CONSTRAINT", Token.CONSTRAINT);
        map.put("CREATE", Token.CREATE);
        map.put("DATABASE", Token.DATABASE);
        map.put("DEFAULT", Token.DEFAULT);
        map.put("COLUMN", Token.COLUMN);
        map.put("TABLESPACE", Token.TABLESPACE);
        map.put("PROCEDURE", Token.PROCEDURE);
        map.put("FUNCTION", Token.FUNCTION);
        map.put("DELETE", Token.DELETE);
        map.put("DESC", Token.DESC);
        map.put("DISTINCT", Token.DISTINCT);
        map.put("DROP", Token.DROP);
        map.put("ELSE", Token.ELSE);
        map.put("EXPLAIN", Token.EXPLAIN);
        map.put("EXCEPT", Token.EXCEPT);
        map.put("END", Token.END);
        map.put("ESCAPE", Token.ESCAPE);
        map.put("EXISTS", Token.EXISTS);
        map.put("FOR", Token.FOR);
        map.put("FOREIGN", Token.FOREIGN);
        map.put("FROM", Token.FROM);
        map.put("FULL", Token.FULL);
        map.put("GROUP", Token.GROUP);
        map.put("HAVING", Token.HAVING);
        map.put("IN", Token.IN);
        map.put("INDEX", Token.INDEX);
        map.put("INNER", Token.INNER);
        map.put("INSERT", Token.INSERT);
        map.put("INTERSECT", Token.INTERSECT);
        map.put("INTERVAL", Token.INTERVAL);
        map.put("INTO", Token.INTO);
        map.put("IS", Token.IS);
        map.put("JOIN", Token.JOIN);
        map.put("KEY", Token.KEY);
        map.put("LEFT", Token.LEFT);
        map.put("LIKE", Token.LIKE);
        map.put("LOCK", Token.LOCK);
        map.put("MINUS", Token.MINUS);
        map.put("NOT", Token.NOT);
        map.put("NULL", Token.NULL);
        map.put("ON", Token.ON);
        map.put("OR", Token.OR);
        map.put("ORDER", Token.ORDER);
        map.put("OUTER", Token.OUTER);
        map.put("PRIMARY", Token.PRIMARY);
        map.put("REFERENCES", Token.REFERENCES);
        map.put("RIGHT", Token.RIGHT);
        map.put("SCHEMA", Token.SCHEMA);
        map.put("SELECT", Token.SELECT);
        map.put("SET", Token.SET);
        map.put("SOME", Token.SOME);
        map.put("TABLE", Token.TABLE);
        map.put("THEN", Token.THEN);
        map.put("TRUNCATE", Token.TRUNCATE);
        map.put("UNION", Token.UNION);
        map.put("UNIQUE", Token.UNIQUE);
        map.put("UPDATE", Token.UPDATE);
        map.put("VALUES", Token.VALUES);
        map.put("VIEW", Token.VIEW);
        map.put("SEQUENCE", Token.SEQUENCE);
        map.put("TRIGGER", Token.TRIGGER);
        map.put("USER", Token.USER);
        map.put("WHEN", Token.WHEN);
        map.put("WHERE", Token.WHERE);
        map.put("XOR", Token.XOR);
        map.put("OVER", Token.OVER);
        map.put("TO", Token.TO);
        map.put("USE", Token.USE);
        map.put("REPLACE", Token.REPLACE);
        map.put("COMMENT", Token.COMMENT);
        map.put("COMPUTE", Token.COMPUTE);
        map.put("WITH", Token.WITH);
        map.put("GRANT", Token.GRANT);
        map.put("REVOKE", Token.REVOKE);
        // MySql procedure: add by zz
        map.put("WHILE", Token.WHILE);
        map.put("DO", Token.DO);
        map.put("DECLARE", Token.DECLARE);
        map.put("LOOP", Token.LOOP);
        map.put("LEAVE", Token.LEAVE);
        map.put("ITERATE", Token.ITERATE);
        map.put("REPEAT", Token.REPEAT);
        map.put("UNTIL", Token.UNTIL);
        map.put("OPEN", Token.OPEN);
        map.put("CLOSE", Token.CLOSE);
        map.put("CURSOR", Token.CURSOR);
        map.put("FETCH", Token.FETCH);
        map.put("OUT", Token.OUT);
        map.put("INOUT", Token.INOUT);
    }

    public static void main(String[] args) {
        //isXXXTokenGenerator();
        //skipXXXTokenGenerator();
        IntStream.range(24, 31).forEach(x -> {
            Map<Long, List<Token>> map = Stream.of(Token.values())
                    .filter((t) -> t.name() != null)
                    .collect(Collectors.groupingBy((t) -> {
                                String name = t.name();
                                char size = (char)name.length();
                                int b = 378551;
                                int a = 63689;
                                int seed = 13131;
                                long hash = 0;
                                int low = 0;
                                int high = 0;
                                for(int i=0; i<size; i++) {
                                    char c = name.charAt(i);
                                    //BKDRHash
                                    low = low * seed + c;
                                    //RS Hash
                                    high = high * a + c;
                                    a *= b;
                                };
                                hash = (long)(high & 0x7FFFFFFF) << 32 | (long)(low & 0x7FFFFFFF);
                                return (hash & (0xff << x));
//                            return t.name().chars().sum();
                            }
                    ));
            /*long count = map.entrySet().stream()
                    .filter((k) -> k.getValue().size() > 2)
                    .count();
                    if (count == 0) {
                System.out.println("result = "+x);
            }
                    */
            System.out.println("result = "+x+" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            map.entrySet().stream()
                    //.filter((k) -> k.getValue().size() > 1)
                    .forEach((e) -> System.out.format("%d : %s %n", e.getKey(), e.getValue().toString()));


        });
        //当左移位数为
//        result = 24
//        result = 25
//        result = 26
//        result = 27
//        result = 28
//        result = 29
//        result = 30
//        result = 31
                //.forEach((e) -> System.out.format("%d : %s %n", e.getKey(), e.getValue().toString()));
    }

    /**
     * final boolean isFromToken() {
     * return icNextCharIs('R') && icNextCharIs('O') && icNextCharIs('M') &&
     * nextIsBlank();
     * }
     */
    static void isXXXTokenGenerator() {
        map.keySet().forEach((s) -> {
            String keyword = s.toLowerCase();
            final byte ICMask = (byte) 0xDF;//ignore case mask;
            keyword = String.valueOf(keyword.charAt(0)).toUpperCase() + keyword.substring(1, keyword.length());
            System.out.println(keyword.chars()
                    .skip(1)
                    .mapToObj((i) -> String.format("icNextCharIs('%s')", (char) (i & ICMask)))
                    .collect(Collectors
                            .joining("&&",
                                    String.format("final boolean is%sToken() {\n        return ", keyword),
                                    "&&\n                nextIsBlank();\n   }")));
        });
    }

    /**
     * final void skipReferencesToken() {
     * pos+="References".length();
     * //pos<sqlLength?//越界检查
     * }
     */
    static void skipXXXTokenGenerator() {
        map.keySet().forEach((s) -> {
            String keyword = s.toLowerCase();
            keyword = String.valueOf(keyword.charAt(0)).toUpperCase() + keyword.substring(1, keyword.length());
            System.out.format("final void skip%sToken() {\npos+=%d;\n}%n", keyword, keyword.length());
        });
    }
}
