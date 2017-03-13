package io.mycat;

/**
 * Created by Kaiz on 2017/3/4.
 */
public class IntTokenHash {
    public static final int AS               = 0x46580002;
    public static final int IF               = 0x47650002;
    public static final int ON               = 0x409e0002;

    public static final int FOR              = 0x576e0003;
    public static final int NOT              = 0x68b00003;
    public static final int SQL              = 0x53820003;
    public static final int USE              = 0x0f840003;

    public static final int DROP             = 0xba5d0004;
    public static final int FROM             = 0x992e0004;
    public static final int INTO             = 0xb2e70004;
    public static final int JOIN             = 0xf9700004;
    public static final int LOCK             = 0xed330004;
    public static final int LEFT             = 0xf4e50004;
    public static final int SHOW             = 0xb0300004;

    public static final int ALTER            = 0xb2db0005;
    public static final int GROUP            = 0x43f50005;
    public static final int LIMIT            = 0x450a0005;
    public static final int MYCAT            = 0xdce50005;
    public static final int ORDER            = 0x88da0005;
    public static final int RIGHT            = 0xec610005;
    public static final int TABLE            = 0x03780005;
    public static final int UNION            = 0xcf9d0005;
    public static final int WHERE            = 0x75950005;

    public static final int CATLET           = 0xda600006;
    public static final int CREATE           = 0xdde00006;
    public static final int EXISTS           = 0x2ed70006;
    public static final int UPDATE           = 0x62840006;
    public static final int DELETE           = 0x33e50006;
    public static final int SELECT           = 0x02a10006;
    public static final int INSERT           = 0xa3b10006;
    public static final int IGNORE           = 0x6c360006;
    public static final int SCHEMA           = 0x0c490006;

    public static final int DB_TYPE          = 0xac610007;
    public static final int BALANCE          = 0x8d070007;
    public static final int REPLACE          = 0xa88c0007;
    public static final int DELAYED          = 0x56380007;

    public static final int DATANODE         = 0x045d0008;
    public static final int TRUNCATE         = 0xcb5d0008;

    public static final int PROCEDURE        = 0x11600009;

    public static final int LOW_PRIORITY     = 0xb805000c;

    public static final int HIGH_PRIORITY    = 0x7c6f000d;

}
