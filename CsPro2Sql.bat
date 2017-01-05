@ECHO OFF
java -cp dist\CsPro2Sql.jar;dist\lib\commons-cli-1.3.1.jar;dist\lib\mysql-connector-java-5.1.40-bin.jar cspro2sql.Main %*
