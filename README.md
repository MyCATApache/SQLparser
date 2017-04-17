SQLParser
=========

通过一次遍历提取SQL相关信息的项目，为MYCAT2.0而设计

还需要完善的部分：
* 注解语法catlet、db_type、sql、schema的提取
* DCL、TCL语法支持
* ""和''字符串支持 √
* 注释支持 √
* 生成sql语句hash值
* 生成schema和table name的hash值 √
* 支持获取limit条数 √

SQLContext考虑实现以下接口：
1. sql语句个数（通过 ; 区分） √
2. 单个sql语句中token位置，例如 [select(1), from(15), join(26)]
3. 单个sql语句表名位置及其hash √
4. 单个sql语句库名(与表名相关)位置及其hash √
5. 是否包含注解 √
6. 注解类型 √
7. 注解语句 √
8. sql关键字替换

