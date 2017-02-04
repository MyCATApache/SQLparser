package io.mycat;

/**
 * Created by Administrator on 2017/2/4 0004.
 */
public class TCLParser {
    //TCL
    public static final byte SAVEPOINT_SQL = 10;
    public static final byte ROLLBACK_SQL = 11;
    public static final byte SET_TRANSACTION_SQL = 12;

    /*

    savepoint_statement:
	SAVEPOINT_SYMBOL^ identifier
	| RELEASE_SYMBOL^ SAVEPOINT_SYMBOL identifier
	;
     */
    public void transaction_or_locking_statement(final byte[] sql, int pos, SQLContext sqlContext) {
        pos = skip(sql, pos);
        int SQLLength = sql.length;
        byte ch = sql[pos];
        switch (sql[pos]) {
            case 's':
            case 'S': {//STRTAT
                if ((sql[++pos] & 0xDF) == 'T' && (sql[++pos] & 0xDF) == 'A' && (sql[++pos] & 0xDF) == 'R' && (sql[++pos] & 0xDF) == 'T' && (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                    pos = skip(sql, pos);
                    //TRANSACTION
                    if ((sql[++pos] & 0xDF) == 'T' && (sql[++pos] & 0xDF) == 'R' && (sql[++pos] & 0xDF) == 'A' && (sql[++pos] & 0xDF) == 'N' && (sql[++pos] & 0xDF) == 'S' && (sql[++pos] & 0xDF) == 'A' && (sql[++pos] & 0xDF) == 'C' && (sql[++pos] & 0xDF) == 'T' && (sql[++pos] & 0xDF) == 'I' && (sql[++pos] & 0xDF) == 'O' && (sql[++pos] & 0xDF) == 'N' && (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                        pos = skip(sql, pos);
                        //transaction_characteristic*
                        while ((ch == ';' ? false : true) && pos < SQLLength) {
                            switch (ch) {
                                case 'w':
                                case 'W':
                                    break;
                                case 'c':
                                case 'C':
                                    break;
                                case 's':
                                case 'S': {
                                    //savepoint_statement:SAVEPOINT_SYMBOL
                                    if ((sql[pos] & 0xDF) == 'S' && (sql[pos + 1] & 0xDF) == 'A' && (sql[pos + 2] & 0xDF) == 'V' && (sql[pos + 3] & 0xDF) == 'E' && (sql[pos + 4] & 0xDF) == 'P' && (sql[pos + 5] & 0xDF) == 'O' && (sql[pos + 6] & 0xDF) == 'I' && (sql[pos + 7] & 0xDF) == 'N' && (sql[pos + 8] & 0xDF) == 'T' && (sql[pos + 9] == ' ' || sql[pos + 9] == '\t' || sql[pos + 9] == '\r' || sql[pos + 9] == '\n')) {
                                        pos += 9;
                                        pos = skip(sql, pos);
                                        //identifier
                                        int start = pos;
                                        for (; ; ) {
                                            byte c = sql[pos];
                                            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                                                break;
                                            } else {
                                                ++pos;
                                            }
                                        }
                                        int end = pos;
                                        identifierCollector(sql, start, end);
                                        pos = skip(sql, pos);
                                    }
                                    break;
                                }
                                case 'r'://{SERVER_VERSION >= 50605}? READ_SYMBOL (WRITE_SYMBOL | ONLY_SYMBOL)
                                case 'R':
                                    if ((sql[++pos] & 0xDF) == 'E' && (sql[++pos] & 0xDF) == 'A' && (sql[++pos] & 0xDF) == 'D') {
                                        pos = skip(sql, pos);
                                        if ((ch = (byte) (sql[pos] & 0xDF)) == 'W') {
                                            if ((sql[++pos] & 0xDF) == 'I' && (sql[++pos] & 0xDF) == 'T' && (sql[++pos] & 0xDF) == 'E' && (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n' || sql[pos] == ';')) {
                                                //WRITE_SYMBOL
                                            } else {
                                                //异常路径
                                            }
                                        } else if (ch == 'O' && (sql[++pos] & 0xDF) == 'N' && (sql[++pos] & 0xDF) == 'L' && (sql[++pos] & 0xDF) == 'Y' && (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n' || sql[pos] == ';')) {
                                            //ONLY_SYMBOL
                                        } else {
                                            //异常路径
                                        }
                                    } else if ((sql[pos + 1] & 0xDF) == 'E' && (sql[pos + 2] & 0xDF) == 'L' && (sql[pos + 3] & 0xDF) == 'E' && (sql[pos + 4] & 0xDF) == 'A' && (sql[pos + 5] & 0xDF) == 'S' && (sql[pos + 6] & 0xDF) == 'E' && (sql[pos + 7] == ' ' || sql[pos + 7] == '\t' || sql[pos + 7] == '\r' || sql[pos + 7] == '\n')) {
                                        //savepoint_statement:RELEASE_SYMBOL^ SAVEPOINT_SYMBOL identifier
                                        pos += 7;
                                        pos = skip(sql, pos);
                                        {
                                            //savepoint_statement:SAVEPOINT_SYMBOL
                                            if ((sql[pos] & 0xDF) == 'S' && (sql[pos + 1] & 0xDF) == 'A' && (sql[pos + 2] & 0xDF) == 'V' && (sql[pos + 3] & 0xDF) == 'E' && (sql[pos + 4] & 0xDF) == 'P' && (sql[pos + 5] & 0xDF) == 'O' && (sql[pos + 6] & 0xDF) == 'I' && (sql[pos + 7] & 0xDF) == 'N' && (sql[pos + 8] & 0xDF) == 'T' && (sql[pos + 9] == ' ' || sql[pos + 9] == '\t' || sql[pos + 9] == '\r' || sql[pos + 9] == '\n')) {
                                                pos += 9;
                                                pos = skip(sql, pos);
                                                //identifier
                                                int start = pos;
                                                for (; ; ) {
                                                    byte c = sql[pos];
                                                    if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                                                        break;
                                                    } else {
                                                        ++pos;
                                                    }
                                                }
                                                int end = pos;
                                                identifierCollector(sql, start, end);
                                                pos = skip(sql, pos);
                                            }
                                            break;
                                        }
                                    }
                                    break;
                                default:
                                    ++pos;
                            }
                        }
                    }
                }
                break;
            }
            case 'c':
            case 'C': {//COMMIT_SYMBOL^ WORK_SYMBOL? (AND_SYMBOL NO_SYMBOL? CHAIN_SYMBOL)? (NO_SYMBOL? RELEASE_SYMBOL)?
                if ((sql[++pos] & 0xDF) == 'O' && (sql[++pos] & 0xDF) == 'M' && (sql[++pos] & 0xDF) == 'M' && (sql[++pos] & 0xDF) == 'I' && (sql[++pos] & 0xDF) == 'T' && (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                    pos = skip(sql, pos);
                    if ((sql[pos] & 0xDF) == 'W' && (sql[pos + 1] & 0xDF) == 'O' && (sql[pos + 2] & 0xDF) == 'W' && (sql[pos + 3] & 0xDF) == 'K' && (sql[pos + 4] == ' ' || sql[pos + 4] == '\t' || sql[pos + 4] == '\r' || sql[pos + 4] == '\n')) {
                        //WORK_SYMBOL
                        pos += 4;
                        pos = skip(sql, pos);
                    }
                    if ((sql[pos] & 0xDF) == 'A' && (sql[pos + 1] & 0xDF) == 'N' && (sql[pos + 2] & 0xDF) == 'D' && (sql[pos + 4] == ' ' || sql[pos + 4] == '\t' || sql[pos + 4] == '\r' || sql[pos + 4] == '\n')) {
                        pos += 4;
                        pos = skip(sql, pos);
                        //AND_SYMBOL 同下
                        if ((sql[pos] & 0xDF) == 'N' && (sql[pos + 1] & 0xDF) == 'O' && (sql[pos + 2] == ' ' || sql[pos + 2] == '\t' || sql[pos + 2] == '\r' || sql[pos + 2] == '\n')) {
                            pos += 2;
                            //NO_SYMBOL?
                            pos = skip(sql, pos);
                            if ((sql[pos] & 0xDF) == 'C' && (sql[pos + 1] & 0xDF) == 'H' && (sql[pos + 2] & 0xDF) == 'A' && (sql[pos + 3] & 0xDF) == 'I' && (sql[pos + 4] & 0xDF) == 'N' && (sql[pos + 5] == ' ' || sql[pos + 5] == '\t' || sql[pos + 5] == '\r' || sql[pos + 5] == '\n')) {
                                pos += 5;
                                pos = skip(sql, pos);
                                //CHAIN_SYMBOL
                            } else {
                                //非法路径
                            }
                            pos = skip(sql, pos);
                        }
                    }
                    if ((sql[pos] & 0xDF) == 'N' && (sql[pos + 1] & 0xDF) == 'O' && (sql[pos + 2] == ' ' || sql[pos + 2] == '\t' || sql[pos + 2] == '\r' || sql[pos + 2] == '\n')) {
                        //NO_SYMBOL 需要语义分析
                        pos += 3;
                        pos = skip(sql, pos);
                    }
                    if ((sql[pos] & 0xDF) == 'R' && (sql[pos + 1] & 0xDF) == 'E' && (sql[pos + 2] & 0xDF) == 'L' && (sql[pos + 3] & 0xDF) == 'E' && (sql[pos + 4] & 0xDF) == 'A' && (sql[pos + 5] & 0xDF) == 'S' && (sql[pos + 6] & 0xDF) == 'E' && (sql[pos + 7] == ' ' || sql[pos + 7] == '\t' || sql[pos + 7] == '\r' || sql[pos + 7] == '\n')) {
                        //RELEASE_SYMBOL 需要语义分析
                        pos += 7;
                        pos = skip(sql, pos);
                    }
                } else {

                }
                break;
            }
            case 'r':
            case 'R': {
                if ((sql[++pos] & 0xDF) == 'O' && (sql[++pos] & 0xDF) == 'L' && (sql[++pos] & 0xDF) == 'L' && (sql[++pos] & 0xDF) == 'B' && (sql[++pos] & 0xDF) == 'A' && (sql[++pos] & 0xDF) == 'C' && (sql[++pos] & 0xDF) == 'K' && (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n')) {
                    pos = skip(sql, pos);
                    if ((sql[pos] & 0xDF) == 'W' && (sql[pos + 1] & 0xDF) == 'O' && (sql[pos + 2] & 0xDF) == 'W' && (sql[pos + 3] & 0xDF) == 'K' && (sql[pos + 4] == ' ' || sql[pos + 4] == '\t' || sql[pos + 4] == '\r' || sql[pos + 4] == '\n')) {
                        //WORK_SYMBOL
                        pos += 4;
                        pos = skip(sql, pos);
                    }
                    switch (sql[pos]) {
                        case 'a':
                        case 'A':
                            if ((sql[pos + 1] & 0xDF) == 'N' && (sql[pos + 2] & 0xDF) == 'D' && (sql[pos + 4] == ' ' || sql[pos + 4] == '\t' || sql[pos + 4] == '\r' || sql[pos + 4] == '\n')) {
                                pos += 4;
                                pos = skip(sql, pos);
                                //AND_SYMBOL 同上
                                if ((sql[pos] & 0xDF) == 'N' && (sql[pos + 1] & 0xDF) == 'O' && (sql[pos + 2] == ' ' || sql[pos + 2] == '\t' || sql[pos + 2] == '\r' || sql[pos + 2] == '\n')) {
                                    pos += 2;
                                    //NO_SYMBOL?
                                    pos = skip(sql, pos);
                                    if ((sql[pos] & 0xDF) == 'C' && (sql[pos + 1] & 0xDF) == 'H' && (sql[pos + 2] & 0xDF) == 'A' && (sql[pos + 3] & 0xDF) == 'I' && (sql[pos + 4] & 0xDF) == 'N' && (sql[pos + 5] == ' ' || sql[pos + 5] == '\t' || sql[pos + 5] == '\r' || sql[pos + 5] == '\n')) {
                                        pos += 5;
                                        pos = skip(sql, pos);
                                        //CHAIN_SYMBOL
                                    } else {
                                        //非法路径
                                    }
                                    pos = skip(sql, pos);
                                }
                            }
                            break;
                        case 't':
                        case 'T'://TO_SYMBOL
                            if ((sql[pos + 1] & 0xDF) == 'O' && (sql[pos + 5] == ' ' || sql[pos + 5] == '\t' || sql[pos + 5] == '\r' || sql[pos + 5] == '\n')) {
                                pos += 5;
                                pos = skip(sql, pos);
                                //SAVEPOINT_SYMBOL?
                                if ((sql[pos] & 0xDF) == 'S' && (sql[pos + 1] & 0xDF) == 'A' && (sql[pos + 2] & 0xDF) == 'V' && (sql[pos + 3] & 0xDF) == 'E' && (sql[pos + 4] & 0xDF) == 'P' && (sql[pos + 5] & 0xDF) == 'O' && (sql[pos + 6] & 0xDF) == 'I' && (sql[pos + 7] & 0xDF) == 'N' && (sql[pos + 8] & 0xDF) == 'T' && (sql[pos + 9] == ' ' || sql[pos + 9] == '\t' || sql[pos + 9] == '\r' || sql[pos + 9] == '\n')) {
                                    pos += 9;
                                    pos = skip(sql, pos);

                                }
                                //identifier
                                int start = pos;
                                for (; ; ) {
                                    byte c = sql[pos];
                                    if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                                        break;
                                    } else {
                                        ++pos;
                                    }
                                }
                                int end = pos;
                                identifierCollector(sql, start, end);
                                pos = skip(sql, pos);
                            } else {
                                //非法路径
                            }
                            break;
                        default:
                            break;
                    }
                }
                break;
            }
            case ';':
                return;
            default:
                break;
        }
        final int start = pos;
        while (!((ch = sql[pos]) == ' ' || ch == '\t' || ch == '\r' || ch == '\n') && (ch == ';' ? false : true) && pos < SQLLength) {
            ++pos;
        }
        final int end = pos;
        while (ch == ';' ? false : true && pos < SQLLength) {
            ++pos;
        }
    }

    final void identifierCollector(byte[] sql, int start, int end) {
        System.out.println(new String(sql, start, end - start));
    }

    static int skip(final byte[] sql, int pos) {
        int SQLLength = sql.length;
        byte ch = 0;
        while (((ch = sql[pos]) == ' ' || ch == '\t' || ch == '\r' || ch == '\n') && (ch == ';' ? false : true) && pos < SQLLength) {
            if ('#' == ch) {
                while (++pos < SQLLength) {
                    if (sql[pos] == '\n') {
                        continue;
                    }
                }
            } else if ('-' == ch) {
                if (sql[++pos] == '-') {
                    byte maybeSpace = sql[++pos];
                    if (maybeSpace == ' ' || maybeSpace == '\t' || maybeSpace == '\r') {
                        while (pos < SQLLength) {
                            if (sql[++pos] == '\n') {
                                continue;
                            }
                        }
                    }
                }
            } else if ('/' == ch) {
                if (sql[++pos] == '*') {
                    while (++pos < SQLLength) {
                        if (sql[pos] == '*' && sql[pos + 1] == '/') {
                            continue;
                        }
                    }
                }
            }
            ++pos;
        }
        return pos;
    }

}
