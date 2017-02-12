package io.mycat;

/**
 * Created by Kaiz on 2017/2/6.
 * 判断需要忽略大小写时，需要调用 ic 为前缀的接口，ic 是 ignore case 的简写
 */


final class SQLReader {
    byte[] sql;
    int pos;
    int sqlLength;
    long sqlHash;
    long tblHash;
    int tblPos;
    int tblSize;
    final byte ICMask = (byte)0xDF;//ignore case mask;

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
        return (byte)(sql[pos] & ICMask);
    }

    final boolean icCurCharIs(char c) {
        return (byte)(sql[pos] & ICMask) == c;
    }

    final void move() {
        ++pos;
    }

    final byte nextChar() {
        byte c = sql[++pos];
        return c;
    }

    final byte icNextChar() {
        byte c = (byte)(sql[++pos] & ICMask);
        return c;
    }

    final boolean icNextCharIs(char c) {
        byte s = (byte)(sql[++pos] & ICMask);
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

    }

    final void findNextToken() {

    }

    final int getPos() {
        return pos;
    }

    final boolean nextIsBlank() {
        return (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n');
    }
}
