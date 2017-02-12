package io.mycat;

/**
 * Created by Kaiz on 2017/2/6.
 * 判断需要忽略大小写时，需要调用 ic 为前缀的接口，ic 是 ignore case 的简写
 */


public class SQLReader {
    byte[] sql;
    int pos;
    int sqlLength;
    long sqlHash;
    long tblHash;
    int tblPos;
    int tblSize;
    final byte ICMask = (byte)0xDF;//ignore case mask;

    public void init(byte[] src) {
        pos = 0;
        this.sql = src;
        sqlLength = this.sql.length - 1;
        sqlHash = 0;
        tblHash = 0;
        tblPos = 0;
        tblSize = 0;
    }

    public byte cur() {
        return sql[pos];
    }

    public byte icCur() {
        return (byte)(sql[pos] & ICMask);
    }

    public boolean icCurCharIs(char c) {
        return (byte)(sql[pos] & ICMask) == c;
    }

    public void move() {
        ++pos;
    }

    public byte nextChar() {
        byte c = sql[++pos];
        return c;
    }

    public byte icNextChar() {
        byte c = (byte)(sql[++pos] & ICMask);
        return c;
    }

    boolean icNextCharIs(char c) {
        byte s = (byte)(sql[++pos] & ICMask);
        return s == c;
    }

    boolean nextCharIs(char c) {
        byte s = sql[++pos];
        return s == c;
    }

    public void readTblName() {
        pos++;
    }

    public boolean hasNext() {
        return pos < sqlLength;
    }

    public boolean isSelectToken() {
        if ( icNextCharIs('L') && icNextCharIs('E') && icNextCharIs('C') && icNextCharIs('T') &&
                nextIsBlank() ) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isShowToken() {
        if ( icNextCharIs('O') && icNextCharIs('W') &&
                nextIsBlank() ) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isFromToken() {
        if (icNextCharIs('R') && icNextCharIs('O') && icNextCharIs('M') &&
                nextIsBlank() ) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isJoinToken() {
        if (icNextCharIs('O') && icNextCharIs('I')  && icNextCharIs('N') &&
                nextIsBlank() ) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isMultiLineCommentEndToken() {
        if (nextCharIs('*') && sql[pos + 1] == '/') {
            return true;
        }
        return false;
    }

    public void skipBlank() {

    }

    public void readNumber() {

    }

    public void findNextToken() {

    }

    public int getPos() {
        return pos;
    }

    boolean nextIsBlank() {
        return (sql[++pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\r' || sql[pos] == '\n');
    }
}
