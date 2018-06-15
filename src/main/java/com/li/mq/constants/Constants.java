package com.li.mq.constants;

/**
 * 常量接口
 * @author Administrator
 *
 */
public interface Constants {

	/**
	 * 项目配置相关的常量
	 */
	String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	String JDBC_DATASOURCE_SIZE = "10";
	String JDBC_URL = "jdbc:mysql://192.168.65.130:3306/rabbitmq-spark-streaming";
	String JDBC_USER = "root";
	String JDBC_PASSWORD = "121029";
	String JDBC_URL_PROD = "jdbc.url.prod";
	String JDBC_USER_PROD = "jdbc.user.prod";
	String JDBC_PASSWORD_PROD = "jdbc.password.prod";
	String SPARK_LOCAL = "true";


	
}
