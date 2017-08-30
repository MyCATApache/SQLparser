package io.mycat.mycat2.sqlparser;

/**
 * Created by Kaiz on 2017/2/6.
 * 判断需要忽略大小写时，需要调用 ic 为前缀的接口，ic 是 ignore case 的简写
 */


final class SQLReader {
    private byte[] sql;
    private int pos;
    private int sqlLength;
    private long sqlHash;
    private long tblHash;
    private int tblPos;
    private int tblSize;
    private final byte ICMask = (byte) 0xDF;//ignore case mask;

    final void init(byte[] src) {
        pos = 0;
        this.sql = src;
        sqlLength = this.sql.length - 1;
        sqlHash = 0;
        tblHash = 0;
        tblPos = 0;
        tblSize = 0;
    }

    final byte cur() {
        return sql[pos];
    }

    final byte icCur() {
        return (byte) (sql[pos] & ICMask);
    }

    final boolean icCurCharIs(char c) {
        return (byte) (sql[pos] & ICMask) == c;
    }

    final void move() {
        ++pos;
    }

    final byte nextChar() {
        byte c = sql[++pos];
        return c;
    }

    final byte icNextChar() {
        byte c = (byte) (sql[++pos] & ICMask);
        return c;
    }

    final boolean icNextCharIs(char c) {
        byte s = (byte) (sql[++pos] & ICMask);
        return s == c;
    }

    final boolean nextCharIs(char c) {
        byte s = sql[++pos];
        return s == c;
    }

    final void readTblName() {
        pos++;
    }

    final boolean hasNext() {
        return pos < sqlLength;
    }

    final boolean isSelectToken() {
        return icNextCharIs('L') && icNextCharIs('E') && icNextCharIs('C') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isShowToken() {
        return icNextCharIs('O') && icNextCharIs('W') &&
                nextIsBlank();
    }

    final boolean isFromToken() {
        return icNextCharIs('R') && icNextCharIs('O') && icNextCharIs('M') &&
                nextIsBlank();
    }

    final boolean isJoinToken() {
        return icNextCharIs('O') && icNextCharIs('I') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isMultiLineCommentEndToken() {
        return nextCharIs('*') && sql[pos + 1] == '/';
    }


    final void skipBlank() {

    }

    final void readNumber() {
        int pos = this.pos;
        int startFlag = pos;
        byte ch = sql[pos];
        boolean has = true;
        if (ch == '-' || ch == '+') {
            pos += 1;
            ch = sql[pos];
        }
        while ('0' <= ch && ch <= '9' && has) {
            if (pos <= sqlLength) {
                ch = sql[pos];
                ++pos;
            } else {
                has = false;
            }
        }
        boolean isDouble = false;
        if ((ch == '.') && has) {
            if (sql[pos + 1] == '.') {
                this.pos = pos - 1;
                limitNumberCollector(startFlag, this.pos);
                return;
            }
            pos += 2;
            ch = sql[pos];
            isDouble = true;
            while ('0' <= ch && ch <= '9' && has) {
                if (pos <= sqlLength) {
                    ch = sql[pos];
                    ++pos;
                } else {
                    has = false;
                }
            }
        }
        if ((ch == 'e' || ch == 'E') && has) {
            pos += 2;
            ch = sql[pos];
            if (ch == '+' || ch == '-') {
                pos += 2;
                ch = sql[pos];
            }
            while (('0' <= ch && ch <= '9') && has) {
                if (pos <= sqlLength) {
                    ch = sql[pos];
                    ++pos;
                } else {
                    has = false;
                }
            }
            isDouble = true;
        }
        this.pos = has ? pos - 1 : pos;
        if (isDouble) {
            //LITERAL_FLOAT;
        } else {
            //LITERAL_INT;
        }
        limitNumberCollector(startFlag, this.pos);
    }

    final void limitParamMarkerCollector() {
        System.out.println("?");
    }

    final void limitNumberCollector(int start, int end) {
        System.out.println(new String(sql, start, end - start));
    }

    final void findNextToken() {

    }

    final int getPos() {
        return pos;
    }

    final boolean nextIsBlank() {
        return (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n');
    }

    ////////////////////////////////////////////////
    final boolean isCreateToken() {
        return icNextCharIs('R') && icNextCharIs('E') && icNextCharIs('A') && icNextCharIs('T') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isTriggerToken() {
        return icNextCharIs('R') && icNextCharIs('I') && icNextCharIs('G') && icNextCharIs('G') && icNextCharIs('E') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isLockToken() {
        return icNextCharIs('O') && icNextCharIs('C') && icNextCharIs('K') &&
                nextIsBlank();
    }

    final boolean isBetweenToken() {
        return icNextCharIs('E') && icNextCharIs('T') && icNextCharIs('W') && icNextCharIs('E') && icNextCharIs('E') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isCloseToken() {
        return icNextCharIs('L') && icNextCharIs('O') && icNextCharIs('S') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isMinusToken() {
        return icNextCharIs('I') && icNextCharIs('N') && icNextCharIs('U') && icNextCharIs('S') &&
                nextIsBlank();
    }

    final boolean isTableToken() {
        return icNextCharIs('A') && icNextCharIs('B') && icNextCharIs('L') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isWhenToken() {
        return icNextCharIs('H') && icNextCharIs('E') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isExplainToken() {
        return icNextCharIs('X') && icNextCharIs('P') && icNextCharIs('L') && icNextCharIs('A') && icNextCharIs('I') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isElseToken() {
        return icNextCharIs('L') && icNextCharIs('S') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isInnerToken() {
        return icNextCharIs('N') && icNextCharIs('N') && icNextCharIs('E') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isCastToken() {
        return icNextCharIs('A') && icNextCharIs('S') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isLeftToken() {
        return icNextCharIs('E') && icNextCharIs('F') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isInToken() {
        return icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isDistinctToken() {
        return icNextCharIs('I') && icNextCharIs('S') && icNextCharIs('T') && icNextCharIs('I') && icNextCharIs('N') && icNextCharIs('C') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isIsToken() {
        return icNextCharIs('S') &&
                nextIsBlank();
    }

    final boolean isWhereToken() {
        return icNextCharIs('H') && icNextCharIs('E') && icNextCharIs('R') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isFunctionToken() {
        return icNextCharIs('U') && icNextCharIs('N') && icNextCharIs('C') && icNextCharIs('T') && icNextCharIs('I') && icNextCharIs('O') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isCaseToken() {
        return icNextCharIs('A') && icNextCharIs('S') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isOutToken() {
        return icNextCharIs('U') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isAsToken() {
        return icNextCharIs('S') &&
                nextIsBlank();
    }

    final boolean isDatabaseToken() {
        return icNextCharIs('A') && icNextCharIs('T') && icNextCharIs('A') && icNextCharIs('B') && icNextCharIs('A') && icNextCharIs('S') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isCheckToken() {
        return icNextCharIs('H') && icNextCharIs('E') && icNextCharIs('C') && icNextCharIs('K') &&
                nextIsBlank();
    }

    final boolean isThenToken() {
        return icNextCharIs('H') && icNextCharIs('E') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isXorToken() {
        return icNextCharIs('O') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isKeyToken() {
        return icNextCharIs('E') && icNextCharIs('Y') &&
                nextIsBlank();
    }

    final boolean isAlterToken() {
        return icNextCharIs('L') && icNextCharIs('T') && icNextCharIs('E') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isIntoToken() {
        return icNextCharIs('N') && icNextCharIs('T') && icNextCharIs('O') &&
                nextIsBlank();
    }

    final boolean isSetToken() {
        return icNextCharIs('E') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isRepeatToken() {
        return icNextCharIs('E') && icNextCharIs('P') && icNextCharIs('E') && icNextCharIs('A') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isConstraintToken() {
        return icNextCharIs('O') && icNextCharIs('N') && icNextCharIs('S') && icNextCharIs('T') && icNextCharIs('R') && icNextCharIs('A') && icNextCharIs('I') && icNextCharIs('N') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isCommentToken() {
        return icNextCharIs('O') && icNextCharIs('M') && icNextCharIs('M') && icNextCharIs('E') && icNextCharIs('N') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isAscToken() {
        return icNextCharIs('S') && icNextCharIs('C') &&
                nextIsBlank();
    }

    final boolean isGroupToken() {
        return icNextCharIs('R') && icNextCharIs('O') && icNextCharIs('U') && icNextCharIs('P') &&
                nextIsBlank();
    }

    final boolean isOrderToken() {
        return icNextCharIs('R') && icNextCharIs('D') && icNextCharIs('E') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isDeleteToken() {
        return icNextCharIs('E') && icNextCharIs('L') && icNextCharIs('E') && icNextCharIs('T') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isByToken() {
        return icNextCharIs('Y') &&
                nextIsBlank();
    }

    final boolean isRightToken() {
        return icNextCharIs('I') && icNextCharIs('G') && icNextCharIs('H') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isUpdateToken() {
        return icNextCharIs('P') && icNextCharIs('D') && icNextCharIs('A') && icNextCharIs('T') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isValuesToken() {
        return icNextCharIs('A') && icNextCharIs('L') && icNextCharIs('U') && icNextCharIs('E') && icNextCharIs('S') &&
                nextIsBlank();
    }

    final boolean isIntervalToken() {
        return icNextCharIs('N') && icNextCharIs('T') && icNextCharIs('E') && icNextCharIs('R') && icNextCharIs('V') && icNextCharIs('A') && icNextCharIs('L') &&
                nextIsBlank();
    }

    final boolean isFetchToken() {
        return icNextCharIs('E') && icNextCharIs('T') && icNextCharIs('C') && icNextCharIs('H') &&
                nextIsBlank();
    }

    final boolean isProcedureToken() {
        return icNextCharIs('R') && icNextCharIs('O') && icNextCharIs('C') && icNextCharIs('E') && icNextCharIs('D') && icNextCharIs('U') && icNextCharIs('R') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isComputeToken() {
        return icNextCharIs('O') && icNextCharIs('M') && icNextCharIs('P') && icNextCharIs('U') && icNextCharIs('T') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isRevokeToken() {
        return icNextCharIs('E') && icNextCharIs('V') && icNextCharIs('O') && icNextCharIs('K') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isUseToken() {
        return icNextCharIs('S') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isOpenToken() {
        return icNextCharIs('P') && icNextCharIs('E') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isToToken() {
        return icNextCharIs('O') &&
                nextIsBlank();
    }

    final boolean isUnionToken() {
        return icNextCharIs('N') && icNextCharIs('I') && icNextCharIs('O') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isTruncateToken() {
        return icNextCharIs('R') && icNextCharIs('U') && icNextCharIs('N') && icNextCharIs('C') && icNextCharIs('A') && icNextCharIs('T') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isCursorToken() {
        return icNextCharIs('U') && icNextCharIs('R') && icNextCharIs('S') && icNextCharIs('O') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isAllToken() {
        return icNextCharIs('L') && icNextCharIs('L') &&
                nextIsBlank();
    }

    final boolean isColumnToken() {
        return icNextCharIs('O') && icNextCharIs('L') && icNextCharIs('U') && icNextCharIs('M') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isLoopToken() {
        return icNextCharIs('O') && icNextCharIs('O') && icNextCharIs('P') &&
                nextIsBlank();
    }

    final boolean isDoToken() {
        return icNextCharIs('O') &&
                nextIsBlank();
    }

    final boolean isViewToken() {
        return icNextCharIs('I') && icNextCharIs('E') && icNextCharIs('W') &&
                nextIsBlank();
    }

    final boolean isDescToken() {
        return icNextCharIs('E') && icNextCharIs('S') && icNextCharIs('C') &&
                nextIsBlank();
    }

    final boolean isIndexToken() {
        return icNextCharIs('N') && icNextCharIs('D') && icNextCharIs('E') && icNextCharIs('X') &&
                nextIsBlank();
    }

    final boolean isReplaceToken() {
        return icNextCharIs('E') && icNextCharIs('P') && icNextCharIs('L') && icNextCharIs('A') && icNextCharIs('C') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isDisableToken() {
        return icNextCharIs('I') && icNextCharIs('S') && icNextCharIs('A') && icNextCharIs('B') && icNextCharIs('L') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isNullToken() {
        return icNextCharIs('U') && icNextCharIs('L') && icNextCharIs('L') &&
                nextIsBlank();
    }

    final boolean isForToken() {
        return icNextCharIs('O') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isUniqueToken() {
        return icNextCharIs('N') && icNextCharIs('I') && icNextCharIs('Q') && icNextCharIs('U') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isIterateToken() {
        return icNextCharIs('T') && icNextCharIs('E') && icNextCharIs('R') && icNextCharIs('A') && icNextCharIs('T') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isEnableToken() {
        return icNextCharIs('N') && icNextCharIs('A') && icNextCharIs('B') && icNextCharIs('L') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isExceptToken() {
        return icNextCharIs('X') && icNextCharIs('C') && icNextCharIs('E') && icNextCharIs('P') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isTablespaceToken() {
        return icNextCharIs('A') && icNextCharIs('B') && icNextCharIs('L') && icNextCharIs('E') && icNextCharIs('S') && icNextCharIs('P') && icNextCharIs('A') && icNextCharIs('C') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isFullToken() {
        return icNextCharIs('U') && icNextCharIs('L') && icNextCharIs('L') &&
                nextIsBlank();
    }

    final boolean isNotToken() {
        return icNextCharIs('O') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isLikeToken() {
        return icNextCharIs('I') && icNextCharIs('K') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isAndToken() {
        return icNextCharIs('N') && icNextCharIs('D') &&
                nextIsBlank();
    }

    final boolean isEndToken() {
        return icNextCharIs('N') && icNextCharIs('D') &&
                nextIsBlank();
    }

    final boolean isInsertToken() {
        return icNextCharIs('N') && icNextCharIs('S') && icNextCharIs('E') && icNextCharIs('R') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isHavingToken() {
        return icNextCharIs('A') && icNextCharIs('V') && icNextCharIs('I') && icNextCharIs('N') && icNextCharIs('G') &&
                nextIsBlank();
    }

    final boolean isInoutToken() {
        return icNextCharIs('N') && icNextCharIs('O') && icNextCharIs('U') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isAnyToken() {
        return icNextCharIs('N') && icNextCharIs('Y') &&
                nextIsBlank();
    }

    final boolean isDropToken() {
        return icNextCharIs('R') && icNextCharIs('O') && icNextCharIs('P') &&
                nextIsBlank();
    }

    final boolean isSomeToken() {
        return icNextCharIs('O') && icNextCharIs('M') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isForeignToken() {
        return icNextCharIs('O') && icNextCharIs('R') && icNextCharIs('E') && icNextCharIs('I') && icNextCharIs('G') && icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isSchemaToken() {
        return icNextCharIs('C') && icNextCharIs('H') && icNextCharIs('E') && icNextCharIs('M') && icNextCharIs('A') &&
                nextIsBlank();
    }

    final boolean isSequenceToken() {
        return icNextCharIs('E') && icNextCharIs('Q') && icNextCharIs('U') && icNextCharIs('E') && icNextCharIs('N') && icNextCharIs('C') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isLeaveToken() {
        return icNextCharIs('E') && icNextCharIs('A') && icNextCharIs('V') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isOuterToken() {
        return icNextCharIs('U') && icNextCharIs('T') && icNextCharIs('E') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isOnToken() {
        return icNextCharIs('N') &&
                nextIsBlank();
    }

    final boolean isOrToken() {
        return icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isExistsToken() {
        return icNextCharIs('X') && icNextCharIs('I') && icNextCharIs('S') && icNextCharIs('T') && icNextCharIs('S') &&
                nextIsBlank();
    }

    final boolean isPrimaryToken() {
        return icNextCharIs('R') && icNextCharIs('I') && icNextCharIs('M') && icNextCharIs('A') && icNextCharIs('R') && icNextCharIs('Y') &&
                nextIsBlank();
    }

    final boolean isIntersectToken() {
        return icNextCharIs('N') && icNextCharIs('T') && icNextCharIs('E') && icNextCharIs('R') && icNextCharIs('S') && icNextCharIs('E') && icNextCharIs('C') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isUserToken() {
        return icNextCharIs('S') && icNextCharIs('E') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isUntilToken() {
        return icNextCharIs('N') && icNextCharIs('T') && icNextCharIs('I') && icNextCharIs('L') &&
                nextIsBlank();
    }

    final boolean isEscapeToken() {
        return icNextCharIs('S') && icNextCharIs('C') && icNextCharIs('A') && icNextCharIs('P') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isWithToken() {
        return icNextCharIs('I') && icNextCharIs('T') && icNextCharIs('H') &&
                nextIsBlank();
    }

    final boolean isOverToken() {
        return icNextCharIs('V') && icNextCharIs('E') && icNextCharIs('R') &&
                nextIsBlank();
    }

    final boolean isGrantToken() {
        return icNextCharIs('R') && icNextCharIs('A') && icNextCharIs('N') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isDeclareToken() {
        return icNextCharIs('E') && icNextCharIs('C') && icNextCharIs('L') && icNextCharIs('A') && icNextCharIs('R') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isWhileToken() {
        return icNextCharIs('H') && icNextCharIs('I') && icNextCharIs('L') && icNextCharIs('E') &&
                nextIsBlank();
    }

    final boolean isDefaultToken() {
        return icNextCharIs('E') && icNextCharIs('F') && icNextCharIs('A') && icNextCharIs('U') && icNextCharIs('L') && icNextCharIs('T') &&
                nextIsBlank();
    }

    final boolean isReferencesToken() {
        return icNextCharIs('E') && icNextCharIs('F') && icNextCharIs('E') && icNextCharIs('R') && icNextCharIs('E') && icNextCharIs('N') && icNextCharIs('C') && icNextCharIs('E') && icNextCharIs('S') &&
                nextIsBlank();
    }

    final void skipCreateToken() {
        pos += 6;
    }

    final void skipTriggerToken() {
        pos += 7;
    }

    final void skipJoinToken() {
        pos += 4;
    }

    final void skipLockToken() {
        pos += 4;
    }

    final void skipBetweenToken() {
        pos += 7;
    }

    final void skipCloseToken() {
        pos += 5;
    }

    final void skipMinusToken() {
        pos += 5;
    }

    final void skipTableToken() {
        pos += 5;
    }

    final void skipWhenToken() {
        pos += 4;
    }

    final void skipExplainToken() {
        pos += 7;
    }

    final void skipElseToken() {
        pos += 4;
    }

    final void skipInnerToken() {
        pos += 5;
    }

    final void skipCastToken() {
        pos += 4;
    }

    final void skipLeftToken() {
        pos += 4;
    }

    final void skipInToken() {
        pos += 2;
    }

    final void skipDistinctToken() {
        pos += 8;
    }

    final void skipIsToken() {
        pos += 2;
    }

    final void skipWhereToken() {
        pos += 5;
    }

    final void skipFunctionToken() {
        pos += 8;
    }

    final void skipCaseToken() {
        pos += 4;
    }

    final void skipOutToken() {
        pos += 3;
    }

    final void skipAsToken() {
        pos += 2;
    }

    final void skipDatabaseToken() {
        pos += 8;
    }

    final void skipCheckToken() {
        pos += 5;
    }

    final void skipThenToken() {
        pos += 4;
    }

    final void skipXorToken() {
        pos += 3;
    }

    final void skipKeyToken() {
        pos += 3;
    }

    final void skipAlterToken() {
        pos += 5;
    }

    final void skipIntoToken() {
        pos += 4;
    }

    final void skipSetToken() {
        pos += 3;
    }

    final void skipRepeatToken() {
        pos += 6;
    }

    final void skipConstraintToken() {
        pos += 10;
    }

    final void skipCommentToken() {
        pos += 7;
    }

    final void skipAscToken() {
        pos += 3;
    }

    final void skipGroupToken() {
        pos += 5;
    }

    final void skipOrderToken() {
        pos += 5;
    }

    final void skipDeleteToken() {
        pos += 6;
    }

    final void skipByToken() {
        pos += 2;
    }

    final void skipRightToken() {
        pos += 5;
    }

    final void skipUpdateToken() {
        pos += 6;
    }

    final void skipValuesToken() {
        pos += 6;
    }

    final void skipIntervalToken() {
        pos += 8;
    }

    final void skipFetchToken() {
        pos += 5;
    }

    final void skipProcedureToken() {
        pos += 9;
    }

    final void skipComputeToken() {
        pos += 7;
    }

    final void skipRevokeToken() {
        pos += 6;
    }

    final void skipUseToken() {
        pos += 3;
    }

    final void skipSelectToken() {
        pos += 6;
    }

    final void skipOpenToken() {
        pos += 4;
    }

    final void skipToToken() {
        pos += 2;
    }

    final void skipUnionToken() {
        pos += 5;
    }

    final void skipTruncateToken() {
        pos += 8;
    }

    final void skipCursorToken() {
        pos += 6;
    }

    final void skipAllToken() {
        pos += 3;
    }

    final void skipColumnToken() {
        pos += 6;
    }

    final void skipLoopToken() {
        pos += 4;
    }

    final void skipFromToken() {
        pos += 4;
    }

    final void skipDoToken() {
        pos += 2;
    }

    final void skipViewToken() {
        pos += 4;
    }

    final void skipDescToken() {
        pos += 4;
    }

    final void skipIndexToken() {
        pos += 5;
    }

    final void skipReplaceToken() {
        pos += 7;
    }

    final void skipDisableToken() {
        pos += 7;
    }

    final void skipNullToken() {
        pos += 4;
    }

    final void skipForToken() {
        pos += 3;
    }

    final void skipUniqueToken() {
        pos += 6;
    }

    final void skipIterateToken() {
        pos += 7;
    }

    final void skipEnableToken() {
        pos += 6;
    }

    final void skipExceptToken() {
        pos += 6;
    }

    final void skipTablespaceToken() {
        pos += 10;
    }

    final void skipFullToken() {
        pos += 4;
    }

    final void skipNotToken() {
        pos += 3;
    }

    final void skipLikeToken() {
        pos += 4;
    }

    final void skipAndToken() {
        pos += 3;
    }

    final void skipEndToken() {
        pos += 3;
    }

    final void skipInsertToken() {
        pos += 6;
    }

    final void skipHavingToken() {
        pos += 6;
    }

    final void skipInoutToken() {
        pos += 5;
    }

    final void skipAnyToken() {
        pos += 3;
    }

    final void skipDropToken() {
        pos += 4;
    }

    final void skipSomeToken() {
        pos += 4;
    }

    final void skipForeignToken() {
        pos += 7;
    }

    final void skipSchemaToken() {
        pos += 6;
    }

    final void skipSequenceToken() {
        pos += 8;
    }

    final void skipLeaveToken() {
        pos += 5;
    }

    final void skipOuterToken() {
        pos += 5;
    }

    final void skipOnToken() {
        pos += 2;
    }

    final void skipOrToken() {
        pos += 2;
    }

    final void skipExistsToken() {
        pos += 6;
    }

    final void skipPrimaryToken() {
        pos += 7;
    }

    final void skipIntersectToken() {
        pos += 9;
    }

    final void skipUserToken() {
        pos += 4;
    }

    final void skipUntilToken() {
        pos += 5;
    }

    final void skipEscapeToken() {
        pos += 6;
    }

    final void skipWithToken() {
        pos += 4;
    }

    final void skipOverToken() {
        pos += 4;
    }

    final void skipGrantToken() {
        pos += 5;
    }

    final void skipDeclareToken() {
        pos += 7;
    }

    final void skipWhileToken() {
        pos += 5;
    }

    final void skipDefaultToken() {
        pos += 7;
    }

    final void skipReferencesToken() {
        pos += 10;
    }


}
