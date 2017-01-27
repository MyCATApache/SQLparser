package io.mycat;

import java.nio.charset.StandardCharsets;

/**
 * Created by Kaiz on 2017/1/22.
 */
public class SQLParser {
    private final byte BASIC_PARSER = 0;
    private final byte TBL_NAME_PARSER = 1;
    private final byte TBL_ALIAS_FINDER = 2;
    private final byte TBL_ALIAS_PARSER = 3;
    private final byte TBL_COMMA_FINDER = 4;
    private final byte INSERT_OPTIONS_PARSER = 5;
    private final byte TBL_OPTION_PARSER = 6;

    private final byte QUEUE_SIZE = 16;
    private byte[] status_queue = new byte[QUEUE_SIZE]; //by kaiz : 为扩展复杂的解析预留空间，考虑到表名之后的修饰token可能无法预期，将可能需要处理的步骤状态值压入队列中，再从队列中逐一处理
    private byte queue_pos = 0;
    private int pos;
    private int SQLLength;
    private int resultSize = 1;
    private byte[] sql;
    private SQLContext context;
    private int tokenCount = 0;
    private int tblTokenPos = 0; //by kaiz : 用于处理 tbl_A a,tbl_B b 的情况

    //static byte[] status_queue = new byte[QUEUE_SIZE];
    void parse(final byte[] bytes, SQLContext sqlContext) {
        sql = bytes;
        context = sqlContext;
        SQLLength = sql.length - 1;
        pos = 0;
        resultSize = 1;
        queue_pos = 0;
        tokenCount = 0;
        status_queue[queue_pos] = BASIC_PARSER;
        context.setCurBuffer(sql);
        while (pos < SQLLength) {  //by kaiz : 考虑到将来可能要用unsafe直接访问，所以越界判断都提前了
            switch (status_queue[queue_pos]) {
                case BASIC_PARSER:
                    //by kaiz : 清空状态数组，两种情况需要进行效率对比（毕竟只有16字节）
                    for (queue_pos = 1; status_queue[queue_pos] != BASIC_PARSER && queue_pos < QUEUE_SIZE; status_queue[queue_pos++] = BASIC_PARSER)
                        ;
                    queue_pos = 0;
                    basic_loop:
                    while (pos < SQLLength) {
                        switch (sql[pos]) {
                            case 'F'://FROM
                            case 'f':
                                tokenCount++;//by kaiz : 所有的token遍历时，都记得要加 tokenCount，在后面token距离计算时会用到
                                if ((sql[++pos] & 0x5f) == 'R' && (sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'M' &&
                                        (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                    //by kaiz : 将接下来需要处理的状态按顺序加入队列
                                    status_queue[0] = TBL_NAME_PARSER;
                                    status_queue[1] = TBL_ALIAS_FINDER;
                                    status_queue[2] = TBL_ALIAS_PARSER;
                                    status_queue[3] = TBL_COMMA_FINDER;
                                    break basic_loop;//by kaiz : 设置好状态列表后，可以跳出当前处理，进入接下来的状态处理
                                } else {
                                    findNextToken(false); //by kaiz : 如果不是需要处理的token，需要在此消耗掉，避免诸如 from_delete 之类的token被误判
                                }
                                break;
                            case 'J'://JOIN
                            case 'j':
                                tokenCount++;
                                if ((sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'N' &&
                                        (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                    status_queue[0] = TBL_NAME_PARSER;//by kaiz : 辅助语句功能型的token不需要设置SQL type
                                    break basic_loop;
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'U'://UPDATE //USE
                            case 'u':
                                tokenCount++;
                                switch (sql[++pos]) {
                                    case 'P':
                                    case 'p':
                                        if ((sql[++pos] & 0x5f) == 'D' && (sql[++pos] & 0x5f) == 'A' && (sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'E' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.UPDATE_SQL);//by kaiz : 主导语句功能的token记得设置SQL Type
                                            status_queue[0] = TBL_NAME_PARSER;
                                            break basic_loop;
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'S':
                                    case 's':
                                        if ((sql[++pos] & 0x5f) == 'E' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.USE_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    default:
                                        findNextToken(false);
                                        break;
                                }
                                break;
                            case 'D'://DELETE //DROP
                            case 'd':
                                tokenCount++;
                                switch ((sql[++pos] & 0x5f)) {
                                    case 'E':
                                        if ((sql[++pos] & 0x5f) == 'L' && (sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'E' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.DELETE_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'R':
                                        if ((sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'P' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.DROP_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    default:
                                        findNextToken(false);
                                }
                                break;
                            case 'S'://SELECT //SHOW
                            case 's':
                                tokenCount++;
                                switch ((sql[++pos] & 0x5f)) {
                                    case 'E':
                                        if ((sql[++pos] & 0x5f) == 'L' && (sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'C' && (sql[++pos] & 0x5f) == 'T' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.SELECT_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'H':
                                        if ((sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'W' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.SHOW_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    default:
                                        findNextToken(false);
                                }
                                break;
                            case 'I': //INSERT //INTO
                            case 'i':
                                tokenCount++;
                                if ((sql[++pos] & 0x5f) == 'N') {
                                    switch ((sql[++pos] & 0x5f)) {
                                        case 'S':
                                            if ((sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'R' && (sql[++pos] & 0x5f) == 'T' &&
                                                    (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                                sqlContext.setSQLType(SQLContext.INSERT_SQL);
                                                status_queue[0] = INSERT_OPTIONS_PARSER;
                                                status_queue[1] = TBL_NAME_PARSER;
                                                break basic_loop;
                                            } else {
                                                findNextToken(false);
                                            }
                                            break;
                                        case 'T':
                                            if ((sql[++pos] & 0x5f) == 'O' &&
                                                    (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                                status_queue[0] = TBL_NAME_PARSER;
                                                break basic_loop;
                                            } else {
                                                findNextToken(false);
                                            }
                                            break;
                                        default:
                                            findNextToken(false);
                                            break;
                                    }
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'L': //LOCK //LIMIT
                            case 'l':
                                tokenCount++;
                                switch ((sql[++pos] & 0x5f)) {
                                    case 'O':
                                        if ((sql[++pos] & 0x5f) == 'C' && (sql[++pos] & 0x5f) == 'K' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.LOCK_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'I':
                                        if ((sql[++pos] & 0x5f) == 'M' && (sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'T' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.LOCK_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    default:
                                        findNextToken(false);
                                }
                                break;
                            case 'A'://ALTER
                            case 'a':
                                tokenCount++;
                                if ((sql[++pos] & 0x5f) == 'L' && (sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'R' &&
                                        (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                    context.setSQLType(SQLContext.ALTER_SQL);
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'C'://CREATE
                            case 'c':
                                tokenCount++;
                                if ((sql[++pos] & 0x5f) == 'R' && (sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'A' && (sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'E' &&
                                        (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                    context.setSQLType(SQLContext.CREATE_SQL);
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'R'://REPLACE
                            case 'r':
                                tokenCount++;
                                if ((sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'P' && (sql[++pos] & 0x5f) == 'L' && (sql[++pos] & 0x5f) == 'A' && (sql[++pos] & 0x5f) == 'C' && (sql[++pos] & 0x5f) == 'E' &&
                                        (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                    context.setSQLType(SQLContext.REPLACE_SQL);
                                    status_queue[0] = INSERT_OPTIONS_PARSER;
                                    status_queue[1] = TBL_NAME_PARSER;
                                    break basic_loop;
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'T'://TABLE //TRUNCATE
                            case 't':
                                tokenCount++;
                                switch (sql[++pos] & 0x5f) {
                                    case 'A':
                                        if ((sql[++pos] & 0x5f) == 'B' && (sql[++pos] & 0x5f) == 'L' && (sql[++pos] & 0x5f) == 'E' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            status_queue[0] = TBL_OPTION_PARSER;
                                            status_queue[1] = TBL_NAME_PARSER;
                                            break basic_loop;
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'R':
                                        if ((sql[++pos] & 0x5f) == 'U' && (sql[++pos] & 0x5f) == 'N' && (sql[++pos] & 0x5f) == 'C' && (sql[++pos] & 0x5f) == 'A' && (sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'E' &&
                                                (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                            context.setSQLType(SQLContext.TRUNCATE_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    default:
                                        findNextToken(false);
                                        break;
                                }

                                break;
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                pos++;
                                break;
                            case ',':
                                pos++;
                                if (tokenCount - tblTokenPos < 2) {
                                    status_queue[0] = TBL_NAME_PARSER;
                                    status_queue[1] = TBL_ALIAS_FINDER;
                                    status_queue[2] = TBL_ALIAS_PARSER;
                                    status_queue[3] = TBL_COMMA_FINDER;
                                    break basic_loop;
                                }
                                break;
                            case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                                ++pos;
                                while (pos < SQLLength) {
                                    if ((sql[pos]) == '\n') {
                                        ++pos;
                                        break;
                                    } else {
                                        ++pos;
                                    }
                                }
                                break;
                            case '-': {
                                // 从‘-- ’序列到行尾。请注意‘-- ’(双破折号)注释风格要求第2个破折号后面至少跟一个空格符(例如空格、tab、换行符等等)。该语法与标准SQL注释语法稍有不同
                                //https://dev.mysql.com/doc/refman/5.7/en/comments.html
                                if (sql[pos + 1] == '-') {
                                    byte maybeSpace = sql[pos + 2];
                                    if (maybeSpace == ' ' || maybeSpace == '\t' || maybeSpace == '\r') {
                                        pos += 3;
                                        while (pos < SQLLength) {
                                            if (sql[pos] == '\n') {
                                                ++pos;
                                                break;
                                            } else {
                                                ++pos;
                                            }
                                        }
                                        break;
                                    }
                                }
                                //goto default;
                            }
                            case '/': {
                                if (sql[pos + 1] == '*') {
                                    /*
                                   这种注释方式还有一种扩展，即当在注释中使用！加上版本号时，只要mysql的当前版本等于或大于该版本号，则该注释中的sql语句将被mysql执行。这种方式只适用于mysql数据库。不具有其他数据库的可移植性。
                                    */
                                    pos += 2;
                                    while (true) {
                                        if (sql[pos] == '*' && sql[pos + 1] == '/') {
                                            pos += 2;
                                            break;
                                        } else {
                                            ++pos;
                                        }
                                    }
                                    break;
                                }
                                //goto default;
                            }
                            default:
                                pos++;
                                tokenCount++;
                                findNextToken(false); //by kaiz : 消耗掉无用token
                                break;
                        }
                    }
                    break;
                case TBL_NAME_PARSER:
                    TblNameParser();
                    break;
                case TBL_ALIAS_FINDER: //by kaiz : 因为可能存在 `schema_name`.`table_name` 或者 table_a as AA, table_b as BB 这种形式的，所以用TBL_ALIAS_FINDER部分用来处理这部分数据
                    TblAliasFinder();
                    break;
                case TBL_ALIAS_PARSER:
                    TblAliasParser();
                    break;
                case TBL_COMMA_FINDER:
                    TblCommaFinder();
                    break;
                case INSERT_OPTIONS_PARSER:
                    InsertOptionsParser();
                    break;
                case TBL_OPTION_PARSER:
                    TableOptionsParser();
                    break;
                default:
            }
        }
    }

    void findNextToken(boolean jump_status) {
        while (pos < SQLLength) {
            switch (sql[pos]) {
                case ' ':
                case '\r':
                case '\t':
                case '\n':
                case '(':
                case ')':
                case ';':
                    jump_status = true;
                    pos++;
                    break;
                case ',':
                    //pos++;
                    return;
                default:
                    if (jump_status) {
                        return;
                    } else {
                        pos++;
                    }
            }
        }
        ;
    }

    private void TblNameParser() {
        finder_loop:
        while (pos < SQLLength) {
            switch (sql[++pos]) {  //过滤表名之前的空格
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                case '`':
                    break;
                case ';':
                case '(':
                    status_queue[queue_pos] = BASIC_PARSER; //如果是括号说明是子查询，直接回到正常解析
                    break finder_loop;
                default:
                    if (context.isTblNameEnd()) {
                        context.setTblNameStart(pos);
                        tokenCount++;
                        resultSize = 1;
                    } else {
                        resultSize++;
                    }
                    name_loop:
                    while (pos < SQLLength) {
                        switch (sql[++pos]) { //by kaiz : 检查表名之后结束串
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                            case '(':
                            case ')':
                            case ';':
                                context.setTblNameSize(resultSize);
                                tblTokenPos = tokenCount;
                                queue_pos++;
                                break finder_loop;
                            case '`':
                                context.setTblNameSize(resultSize);
                                if (sql[++pos] == '.') {
                                    context.pushSchemaName();
                                    break name_loop;
                                } else {
                                    tblTokenPos = tokenCount;
                                    queue_pos++;
                                    break finder_loop;
                                }
                            case ',':
                                context.setTblNameSize(resultSize);
                                tblTokenPos = tokenCount;
                                break name_loop;
                            case '.':
                                context.setTblNameSize(resultSize);
                                context.pushSchemaName();
                                break name_loop;
                            default:
                                resultSize++;
                        }
                    }
                    break finder_loop;
            }
        }
    }

    private void TblAliasFinder() {
        alias_loop:
        while (pos < SQLLength) {
            switch (sql[++pos]) {
                case 'A': //by kaiz : 处理 AS 写法
                case 'a':
                    if ((sql[++pos] & 0x5f) == 'S' &&
                            (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        tokenCount++;
                        queue_pos++;
                    } else if (sql[pos] == ',') {
                        queue_pos = 0;
                    } else {
                        status_queue[queue_pos] = BASIC_PARSER;
                    }
                    break alias_loop;
                case ',':
                    queue_pos = 0; //by kaiz : 和 TBL_COMMA_FINDER 一样的处理，回到队列首，重新开始下一个处理循环
                    break alias_loop;
                case '.': //by kaiz : 处理 . 写法
                    context.pushSchemaName();
                    while (status_queue[--queue_pos] != TBL_NAME_PARSER && queue_pos > 0) ;
                    break alias_loop;
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                default:
                    status_queue[queue_pos] = BASIC_PARSER;
                    break alias_loop;
            }
        }
    }

    private void TblAliasParser() {
        alias_loop:
        while (pos < SQLLength) { //by kaiz : 略过 AS 后空格
            switch (sql[++pos]) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                default:
                    tokenCount++;
                    while (pos < SQLLength) { //by kaiz : 略过别名
                        switch (sql[++pos]) {
                            case ' ':
                            case '\t':
                            case '\r':
                            case '\n':
                                queue_pos++;
                                break alias_loop;
                            case ',':
                                queue_pos = 0;
                                break alias_loop;
                        }
                    }
                    break alias_loop;
            }
        }
    }

    private void TblCommaFinder() {
        comma_loop:
        while (pos < SQLLength) {
            switch (sql[++pos]) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                case ',':
                    queue_pos = 0; //by kaiz : 回到队列首，重新开始下一个处理循环
                    break comma_loop;
                default:
                    status_queue[queue_pos] = BASIC_PARSER; //by kaiz : 回到基本处理流程
                    break comma_loop;
            }
        }
    }

    private void InsertOptionsParser() {
        int tempPos = pos;
        parser_loop:
        while (pos < SQLLength) {
            switch (sql[++pos]) {
                case 'D'://DELAYED
                case 'd':
                    tokenCount++;
                    tempPos = pos;
                    if ((sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'L' && sql[++pos] == 'A' && (sql[++pos] & 0x5f) == 'Y' && (sql[++pos] & 0x5f) == 'E' && (sql[++pos] & 0x5f) == 'D' &&
                            (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        //context.setSQLType(SQLContext.ALTER_SQL);
                        //by kaiz : just pass...
                        //maybe add options later
                    } else {
                        context.setTblNameStart(tempPos);
                        resultSize = 1;
                        queue_pos++;
                        break parser_loop;
                    }
                    break;
                case 'L'://LOW_PRIORITY
                case 'l':
                    tokenCount++;
                    tempPos = pos;
                    if ((sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'W' && sql[++pos] == '_' &&
                            (sql[++pos] & 0x5f) == 'P' && (sql[++pos] & 0x5f) == 'R' && (sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'R' && (sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'Y' &&
                            (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        //context.setSQLType(SQLContext.ALTER_SQL);
                        //by kaiz : just pass...
                        //maybe add options later
                    } else {
                        context.setTblNameStart(tempPos);
                        resultSize = 1;
                        queue_pos++;
                        break parser_loop;
                    }
                    break;
                case 'H'://HIGH_PRIORITY
                case 'h':
                    tokenCount++;
                    tempPos = pos;
                    if ((sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'G' && (sql[++pos] & 0x5f) == 'H' && sql[++pos] == '_' &&
                            (sql[++pos] & 0x5f) == 'P' && (sql[++pos] & 0x5f) == 'R' && (sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'R' && (sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'Y' &&
                            (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        //context.setSQLType(SQLContext.ALTER_SQL);
                        //by kaiz : just pass...
                        //maybe add options later
                    } else {
                        context.setTblNameStart(tempPos);
                        resultSize = 1;
                        queue_pos++;
                        break parser_loop;
                    }
                    break;
                case 'I'://IGNORE
                case 'i':
                    tokenCount++;
                    tempPos = pos;
                    switch (sql[++pos]) {
                        case 'G':
                        case 'g':
                            if ((sql[++pos] & 0x5f) == 'N' && (sql[++pos] & 0x5f) == 'O' && sql[++pos] == 'R' && (sql[++pos] & 0x5f) == 'E' &&
                                    (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                //context.setSQLType(SQLContext.ALTER_SQL);
                                //by kaiz : just pass...
                            } else {
                                context.setTblNameStart(tempPos);
                                resultSize = 1;
                                queue_pos++;
                                break parser_loop;
                            }
                            break;
                        case 'N':
                        case 'n':
                            if ((sql[++pos] & 0x5f) == 'T' && (sql[++pos] & 0x5f) == 'O' &&
                                    (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                                //context.setSQLType(SQLContext.ALTER_SQL);
                                //by kaiz : just pass...
                            } else {
                                context.setTblNameStart(tempPos);
                                resultSize = 1;
                                queue_pos++;
                                break parser_loop;
                            }
                            break;
                        default:
                    }
                    break;
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                default:
                    tokenCount++;
                    context.setTblNameStart(pos);
                    resultSize = 1;
                    queue_pos++;
                    break parser_loop;
            }
        }
    }

    private void TableOptionsParser() {
        int tempPos = pos;
        parser_loop:
        while (pos < SQLLength) {
            switch (sql[++pos]) {
                case 'I'://IF
                case 'i':
                    tokenCount++;
                    tempPos = pos;
                    if ((sql[++pos] & 0x5f) == 'F' &&
                            (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        //context.setSQLType(SQLContext.ALTER_SQL);
                        //by kaiz : just pass...
                        //maybe add options later
                    } else {
                        context.setTblNameStart(tempPos);
                        resultSize = 1;
                        queue_pos++;
                        break parser_loop;
                    }
                    break;
                case 'N'://NOT
                case 'n':
                    tokenCount++;
                    tempPos = pos;
                    if ((sql[++pos] & 0x5f) == 'O' && (sql[++pos] & 0x5f) == 'T' &&
                            (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        //context.setSQLType(SQLContext.ALTER_SQL);
                        //by kaiz : just pass...
                        //maybe add options later
                    } else {
                        context.setTblNameStart(tempPos);
                        resultSize = 1;
                        queue_pos++;
                        break parser_loop;
                    }
                    break;
                case 'E'://EXISTS
                case 'e':
                    tokenCount++;
                    tempPos = pos;
                    if ((sql[++pos] & 0x5f) == 'X' && (sql[++pos] & 0x5f) == 'I' && (sql[++pos] & 0x5f) == 'S' && sql[++pos] == 'T' && sql[++pos] == 'S' &&
                            (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        //context.setSQLType(SQLContext.ALTER_SQL);
                        //by kaiz : just pass...
                        //maybe add options later
                    } else {
                        context.setTblNameStart(tempPos);
                        resultSize = 1;
                        queue_pos++;
                        break parser_loop;
                    }
                    break;
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                default:
                    tokenCount++;
                    context.setTblNameStart(pos);
                    resultSize = 1;
                    queue_pos++;
                    break parser_loop;
            }
        }
    }

    void ReturnBasicParse() {
        status_queue[queue_pos] = BASIC_PARSER;
    }

    void NextStatus() {
        queue_pos++;
    }

    void RollbackStatusToStart() {
        queue_pos = 0;
    }

    void RollbackStatusTo(byte pre_status) {
        while (status_queue[--queue_pos] != pre_status && queue_pos > 0) ;
    }

    static long RunBench(byte[] src, SQLParser parser, SQLContext context) {
        //short[] result = new short[128];
        //  todo:动态解析代码生成器
        //  todo:函数调用
        //  tip:sql越长时间越长
        //  tip:递归好像消耗有点大
        int count = 0;
        long start = System.currentTimeMillis();
        do {
            parser.parse(src, context);
        } while (count++ < 10_000_000);
        return System.currentTimeMillis() - start;
    }

    public static void main(String[] args) {
        long min = 0;
        byte[] src = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));".getBytes(StandardCharsets.UTF_8);//20794
        SQLParser parser = new SQLParser();
        SQLContext context = new SQLContext();
        for (int i = 0; i < 50; i++) {
            System.out.print("Loop " + i + " : ");
            long cur = RunBench(src, parser, context);//无参数优化：7510，加server参数7657，因为是默认就是server jvm，优化流程后6531
            System.out.println(cur);
            if (cur < min || min == 0) {
                min = cur;
            }
        }
        System.out.print("min time : " + min);
    }
}
