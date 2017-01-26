SQLParser
=========

通过一次遍历提取SQL相关信息的项目，为MYCAT2.0而设计

还需要完善的部分：
1. 注解语法catlet、db_type、sql、schema的提取
2. DCL、TCL语法支持
3. ""和''字符串支持
4. 注释支持
5. 生成sql语句hash值
6. 生成schema和table name的hash值
7. 支持获取limit条数

-XX:+PrintCompilation