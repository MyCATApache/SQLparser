package io.mycat.mycat2.sqlparser;

import java.nio.charset.StandardCharsets;

/**
 * Created by Kaiz on 2017/1/22.
 * 考虑设计：1. 第一遍生成 a. sql类型; b. 表名列表; c. 库名列表; d. sql/表名/库名hash值; e. 所有节点位置数组
 * 2. 第二遍生成 a. 查询和子查询之间的关联; b. 查询字段和表之间的关联; c. 获取where判断条件
 * 2017/2/12 优化前效率 SQLBenchmark.SQLParserTest  thrpt   10  1290172.064 ± 36612.015  ops/s
 * 添加 reader 后：SQLBenchmark.SQLParserTest  thrpt   10  1271515.925 ± 39180.293  ops/s
 * 将sql[]和pos操作封装到reader当中对性能影响非常小，接下来会将所有token判断都添加到reader当中
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
    //    private int pos;
    //private int SQLLength;
    private int resultSize = 1;
    //private byte[] sql;
    private SQLContext context;
    private int tokenCount = 0;
    private int tblTokenPos = 0; //by kaiz : 用于处理 tbl_A a,tbl_B b 的情况
    SQLReader reader = new SQLReader();

    //static byte[] status_queue = new byte[QUEUE_SIZE];
    void parse(final byte[] bytes, SQLContext sqlContext) {
        context = sqlContext;
        resultSize = 1;
        queue_pos = 0;
        tokenCount = 0;
        status_queue[queue_pos] = BASIC_PARSER;
        reader.init(bytes);
        context.setCurBuffer(bytes);

        while (reader.hasNext()) {  //by kaiz : 考虑到将来可能要用unsafe直接访问，所以越界判断都提前了
            switch (status_queue[queue_pos]) {
                case BASIC_PARSER:
                    //by kaiz : 清空状态数组，两种情况需要进行效率对比（毕竟只有16字节）
                    for (queue_pos = 1; status_queue[queue_pos] != BASIC_PARSER && queue_pos < QUEUE_SIZE; status_queue[queue_pos++] = BASIC_PARSER)
                        ;
                    queue_pos = 0;
                    basic_loop:
                    while (reader.hasNext()) {
                        switch (reader.cur()) {
                            case 'F'://FROM
                            case 'f':
                                tokenCount++;//by kaiz : 所有的token遍历时，都记得要加 tokenCount，在后面token距离计算时会用到
                                if (reader.isFromToken()) {
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
                                if (reader.isJoinToken()) {
                                    status_queue[0] = TBL_NAME_PARSER;//by kaiz : 辅助语句功能型的token不需要设置SQL type
                                    break basic_loop;
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'U'://UPDATE //USE
                            case 'u':
                                tokenCount++;
                                switch (reader.icNextChar()) {
                                    case 'P':
                                    case 'p':
                                        if (reader.icNextCharIs('D') && reader.icNextCharIs('A') && reader.icNextCharIs('T') && reader.icNextCharIs('E') &&
                                                reader.nextIsBlank()) {
                                            context.setSQLType(SQLContext.UPDATE_SQL);//by kaiz : 主导语句功能的token记得设置SQL Type
                                            status_queue[0] = TBL_NAME_PARSER;
                                            break basic_loop;
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'S':
                                    case 's':
                                        if (reader.icNextCharIs('E') &&
                                                reader.nextIsBlank()) {
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
                                switch (reader.icNextChar()) {
                                    case 'E':
                                        if (reader.icNextCharIs('L') && reader.icNextCharIs('E') && reader.icNextCharIs('T') && reader.icNextCharIs('E') &&
                                                reader.nextIsBlank()) {
                                            context.setSQLType(SQLContext.DELETE_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'R':
                                        if (reader.icNextCharIs('O') && reader.icNextCharIs('P') &&
                                                reader.nextIsBlank()) {
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
                                switch (reader.icNextChar()) {
                                    case 'E':
                                        if (reader.isSelectToken()) {
                                            context.setSQLType(SQLContext.SELECT_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'H':
                                        if (reader.isShowToken()) {
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
                                if (reader.icNextCharIs('N')) {
                                    switch (reader.icNextChar()) {
                                        case 'S':
                                            if (reader.icNextCharIs('E') && reader.icNextCharIs('R') && reader.icNextCharIs('T') &&
                                                    reader.nextIsBlank()) {
                                                sqlContext.setSQLType(SQLContext.INSERT_SQL);
                                                status_queue[0] = INSERT_OPTIONS_PARSER;
                                                status_queue[1] = TBL_NAME_PARSER;
                                                break basic_loop;
                                            } else {
                                                findNextToken(false);
                                            }
                                            break;
                                        case 'T':
                                            if (reader.icNextCharIs('O') &&
                                                    reader.nextIsBlank()) {
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
                                switch (reader.icNextChar()) {
                                    case 'O':
                                        if (reader.icNextCharIs('C') && reader.icNextCharIs('K') &&
                                                reader.nextIsBlank()) {
                                            context.setSQLType(SQLContext.LOCK_SQL);
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'I':
                                        if (reader.icNextCharIs('M') && reader.icNextCharIs('I') && reader.icNextCharIs('T') &&
                                                reader.nextIsBlank()) {
                                            context.setSQLType(SQLContext.LOCK_SQL);
                                            limitClause();
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
                                if (reader.icNextCharIs('L') && reader.icNextCharIs('T') && reader.icNextCharIs('E') && reader.icNextCharIs('R') &&
                                        reader.nextIsBlank()) {
                                    context.setSQLType(SQLContext.ALTER_SQL);
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'C'://CREATE
                            case 'c':
                                tokenCount++;
                                if (reader.icNextCharIs('R') && reader.icNextCharIs('E') && reader.icNextCharIs('A') && reader.icNextCharIs('T') && reader.icNextCharIs('E') &&
                                        reader.nextIsBlank()) {
                                    context.setSQLType(SQLContext.CREATE_SQL);
                                } else {
                                    findNextToken(false);
                                }
                                break;
                            case 'R'://REPLACE
                            case 'r':
                                tokenCount++;
                                if (reader.icNextCharIs('E') && reader.icNextCharIs('P') && reader.icNextCharIs('L') && reader.icNextCharIs('A') && reader.icNextCharIs('C') && reader.icNextCharIs('E') &&
                                        reader.nextIsBlank()) {
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
                                switch (reader.icNextChar()) {
                                    case 'A':
                                        if (reader.icNextCharIs('B') && reader.icNextCharIs('L') && reader.icNextCharIs('E') &&
                                                reader.nextIsBlank()) {
                                            status_queue[0] = TBL_OPTION_PARSER;
                                            status_queue[1] = TBL_NAME_PARSER;
                                            break basic_loop;
                                        } else {
                                            findNextToken(false);
                                        }
                                        break;
                                    case 'R':
                                        if (reader.icNextCharIs('U') && reader.icNextCharIs('N') && reader.icNextCharIs('C') && reader.icNextCharIs('A') && reader.icNextCharIs('T') && reader.icNextCharIs('E') &&
                                                reader.nextIsBlank()) {
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
                                reader.move();
                                break;
                            case ',':
                                reader.move();
                                if (tokenCount - tblTokenPos < 2) {
                                    status_queue[0] = TBL_NAME_PARSER;
                                    status_queue[1] = TBL_ALIAS_FINDER;
                                    status_queue[2] = TBL_ALIAS_PARSER;
                                    status_queue[3] = TBL_COMMA_FINDER;
                                    break basic_loop;
                                }
                                break;
                            case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                                SharpComment();
                                break;
                            case '-':
                                DoubleDashComment();
                                break;
                            case '/':
                                MultiLineComment();
                                break;
                            case '\'':
                            case '"':
                                QuoteString();
                                break;
                            default:
                                reader.move();
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
        while (reader.hasNext()) {
            switch (reader.cur()) {
                case ' ':
                case '\r':
                case '\t':
                case '\n':
                case '(':
                case ')':
                case ';':
                    jump_status = true;
                    reader.move();
                    break;
                case ',':
                    //pos++;
                    return;
                case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                    SharpComment();
                    break;
                case '-':
                    DoubleDashComment();
                    break;
                case '/':
                    MultiLineComment();
                    break;
                default:
                    if (jump_status) {
                        return;
                    } else {
                        reader.move();
                    }
            }
        }
        ;
    }

    private void TblNameParser() {
        finder_loop:
        while (reader.hasNext()) {
            switch (reader.nextChar()) {  //过滤表名之前的空格
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                case '`':
                    break;
                case '#':
                    SharpComment();
                    break;
                case '-':
                    DoubleDashComment();
                    break;
                case '/':
                    MultiLineComment();
                    break;
                case ';':
                case '(':
                    status_queue[queue_pos] = BASIC_PARSER; //如果是括号说明是子查询，直接回到正常解析
                    break finder_loop;
                default:
                    if (context.isTblNameEnd()) {
                        context.setTblNameStart(reader.getPos());
                        tokenCount++;
                        resultSize = 1;
                    } else {
                        resultSize++;
                    }
                    name_loop:
                    while (reader.hasNext()) {
                        switch (reader.nextChar()) { //by kaiz : 检查表名之后结束串
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
                                if (reader.nextCharIs('.')) {
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
        while (reader.hasNext()) {
            switch (reader.nextChar()) {
                case 'A': //by kaiz : 处理 AS 写法
                case 'a':
                    if (reader.icNextCharIs('S') &&
                            reader.nextIsBlank()) {
                        tokenCount++;
                        queue_pos++;
                    } else if (reader.cur() == ',') {
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
                case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                    SharpComment();
                    break;
                case '-':
                    DoubleDashComment();
                    break;
                case '/':
                    MultiLineComment();
                    break;
                default:
                    status_queue[queue_pos] = BASIC_PARSER;
                    break alias_loop;
            }
        }
    }

    private void TblAliasParser() {
        alias_loop:
        while (reader.hasNext()) { //by kaiz : 略过 AS 后空格
            switch (reader.nextChar()) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                    SharpComment();
                    break;
                case '-':
                    DoubleDashComment();
                    break;
                case '/':
                    MultiLineComment();
                    break;
                default:
                    tokenCount++;
                    while (reader.hasNext()) { //by kaiz : 略过别名
                        switch (reader.nextChar()) {
                            case ' ':
                            case '\t':
                            case '\r':
                            case '\n':
                                queue_pos++;
                                break alias_loop;
                            case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                                SharpComment();
                                break;
                            case '-':
                                DoubleDashComment();
                                break;
                            case '/':
                                MultiLineComment();
                                break;
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
        while (reader.hasNext()) {
            switch (reader.nextChar()) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    break;
                case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                    SharpComment();
                    break;
                case '-':
                    DoubleDashComment();
                    break;
                case '/':
                    MultiLineComment();
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
        int tempPos = reader.getPos();
        parser_loop:
        while (reader.hasNext()) {
            switch (reader.nextChar()) {
                case 'D'://DELAYED
                case 'd':
                    tokenCount++;
                    tempPos = reader.getPos();
                    if (reader.icNextCharIs('E') && reader.icNextCharIs('L') && reader.icNextCharIs('A') && reader.icNextCharIs('Y') && reader.icNextCharIs('E') && reader.icNextCharIs('D') &&
                            reader.nextIsBlank()) {
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
                    tempPos = reader.getPos();
                    if (reader.icNextCharIs('O') && reader.icNextCharIs('W') && reader.nextCharIs('_') &&
                            reader.icNextCharIs('P') && reader.icNextCharIs('R') && reader.icNextCharIs('I') && reader.icNextCharIs('O') && reader.icNextCharIs('R') && reader.icNextCharIs('I') && reader.icNextCharIs('T') && reader.icNextCharIs('Y') &&
                            reader.nextIsBlank()) {
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
                    tempPos = reader.getPos();
                    if (reader.icNextCharIs('I') && reader.icNextCharIs('G') && reader.icNextCharIs('H') && reader.nextCharIs('_') &&
                            reader.icNextCharIs('P') && reader.icNextCharIs('R') && reader.icNextCharIs('I') && reader.icNextCharIs('O') && reader.icNextCharIs('R') && reader.icNextCharIs('I') && reader.icNextCharIs('T') && reader.icNextCharIs('Y') &&
                            reader.nextIsBlank()) {
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
                    tempPos = reader.getPos();
                    switch (reader.nextChar()) {
                        case 'G':
                        case 'g':
                            if (reader.icNextCharIs('N') && reader.icNextCharIs('O') && reader.icNextCharIs('R') && reader.icNextCharIs('E') &&
                                    reader.nextIsBlank()) {
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
                            if (reader.icNextCharIs('T') && reader.icNextCharIs('O') &&
                                    reader.nextIsBlank()) {
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
                case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                    SharpComment();
                    break;
                case '-':
                    DoubleDashComment();
                    break;
                case '/':
                    MultiLineComment();
                    break;
                default:
                    tokenCount++;
                    context.setTblNameStart(reader.getPos());
                    resultSize = 1;
                    queue_pos++;
                    break parser_loop;
            }
        }
    }

    private void TableOptionsParser() {
        int tempPos = reader.getPos();
        parser_loop:
        while (reader.hasNext()) {
            switch (reader.nextChar()) {
                case 'I'://IF
                case 'i':
                    tokenCount++;
                    tempPos = reader.getPos();
                    if (reader.icNextCharIs('F') &&
                            reader.nextIsBlank()) {
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
                    tempPos = reader.getPos();
                    if (reader.icNextCharIs('O') && reader.icNextCharIs('T') &&
                            reader.nextIsBlank()) {
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
                    tempPos = reader.getPos();
                    if (reader.icNextCharIs('X') && reader.icNextCharIs('I') && reader.icNextCharIs('S') && reader.icNextCharIs('T') && reader.icNextCharIs('S') &&
                            reader.nextIsBlank()) {
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
                case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                    SharpComment();
                    break;
                case '-':
                    DoubleDashComment();
                    break;
                case '/':
                    MultiLineComment();
                    break;
                default:
                    tokenCount++;
                    context.setTblNameStart(reader.getPos());
                    resultSize = 1;
                    queue_pos++;
                    break parser_loop;
            }
        }
    }

    void DoubleDashComment() {
        // 从‘-- ’序列到行尾。请注意‘-- ’(双破折号)注释风格要求第2个破折号后面至少跟一个空格符(例如空格、tab、换行符等等)。该语法与标准SQL注释语法稍有不同
        //https://dev.mysql.com/doc/refman/5.7/en/comments.html
        if (reader.nextCharIs('-')) {
            byte maybeSpace = reader.nextChar();
            if (maybeSpace == ' ' || maybeSpace == '\t' || maybeSpace == '\r') {
                while (reader.hasNext()) {
                    if (reader.nextCharIs('\n')) {
                        return;
                    }
                }
            }
        }
    }

    void SharpComment() {
        while (reader.hasNext()) {
            if (reader.nextCharIs('\n')) {
                return;
            }
        }
    }

    /*
    这种注释方式还有一种扩展，即当在注释中使用！加上版本号时，
    只要mysql的当前版本等于或大于该版本号，则该注释中的sql语句将被mysql执行。
    这种方式只适用于mysql数据库。不具有其他数据库的可移植性。
    */
    void MultiLineComment() {
        if (reader.nextCharIs('*')) {
            while (reader.hasNext()) {
                if (reader.isMultiLineCommentEndToken()) {
                    return;
                }
            }
        }
    }


    void limitClause() {
        int argCount = 0;
        while (reader.hasNext()) {
            switch (reader.nextChar()) {
                case '+':
                case '-':
                    byte digit = reader.nextChar();
                    if ('0' <= digit && digit <= '9') {
                        reader.readNumber();
                        ++argCount;
                    } else if (digit == '-') {
                        DoubleDashComment();
                    }
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9': {
                    reader.readNumber();
                    ++argCount;
                    break;
                }
                case '?'://PARAM_MARKER
                    reader.limitParamMarkerCollector();
                    ++argCount;
                    break;
                case '#'://"#" 和"–- "属于单行注释，注释范围为该行的结尾
                    SharpComment();
                    break;
                case '/':
                    MultiLineComment();
                    break;
                case ' ':
                case '\r':
                case '\t':
                case '\n':
                    break;
                default:
                    if (argCount == 1) {
                        return;
                    } else {
                        //Identifier
                        ++argCount;
                    }
                    break;
                case 'O':
                case 'o': {
                    if (reader.icNextCharIs('F') && reader.icNextCharIs('F') && reader.icNextCharIs('S') && reader.icNextCharIs('E') && reader.icNextCharIs('T') &&
                            reader.nextIsBlank()) {
                        if (argCount == 1) {
                            //goto    case ','
                        } else {
                            //offset 的普通字符串 ,非关键字,异常路径
                        }
                    } else {
                        //Identifier
                        continue;
                    }
                }
                case ',':
                    if (argCount == 1) {
                        continue;
                    } else {
                        //异常路径
                    }
            }
            if (argCount == 2) {
                return;
            }
        }
    }


    void QuoteString() {
        byte end = reader.cur();
        while (reader.hasNext()) {
            if (reader.nextChar() == end) {
                reader.move();
                return;
            } else if (reader.cur() == '\\') {
                reader.move();
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

//    public int getSQLLength() {
//        return SQLLength;
//    }

    public int getResultSize() {
        return resultSize;
    }
}
