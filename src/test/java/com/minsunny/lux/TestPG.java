package com.minsunny.lux;

import org.junit.jupiter.api.Test;
import org.postgresql.PGStatement;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.*;

public class TestPG {
    static Connection getConnection() throws SQLException {
        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setUser("postgres");
        pgSimpleDataSource.setPassword("example");
        return pgSimpleDataSource.getConnection();

    }
    static Connection getConnectionSimpleMode() throws SQLException {
        PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setUser("postgres");
        pgSimpleDataSource.setPassword("example");
        pgSimpleDataSource.setProperty("preferQueryMode", "simple");
        return pgSimpleDataSource.getConnection();

    }
    @Test
    void test0() throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        statement.execute("""
        drop  table if exists time_table;
        create table time_table (time_type time(6))
        """);
        statement.execute("insert into time_table (time_type) values ('12:12:12.123456')");
        // every time create new PreparedStatement,but don't close it;

        for (int i = 0; i < 7; i++) {
            PreparedStatement ps = connection.prepareStatement("select * from time_table");
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            System.out.println("query times : %s %s PreparedStatement's hascode %s".formatted(i,
                    resultSet.getString(1),ps.hashCode()));
        }
        /**
         * the result
         * query times : 0 12:12:12.123456 PreparedStatement's hascode 594427726
         * query times : 1 12:12:12.123456 PreparedStatement's hascode 1019298652
         * query times : 2 12:12:12.123456 PreparedStatement's hascode 1810899357
         * query times : 3 12:12:12.123456 PreparedStatement's hascode 231786897
         * query times : 4 12:12:12.123456 PreparedStatement's hascode 1595282218
         * query times : 5 12:12:12.123456 PreparedStatement's hascode 1778081847
         * query times : 6 12:12:12.123456 PreparedStatement's hascode 57497692
         */
    }


    @Test
    void test2() throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        statement.execute("""
        drop  table if exists time_table;
        create table time_table (time_type time(6))
        """);
        statement.execute("insert into time_table (time_type) values ('12:12:12.123456')");
        // every time create new PreparedStatement,but close it every time

        for (int i = 0; i < 7; i++) {
            PreparedStatement ps = connection.prepareStatement("select * from time_table");
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            System.out.println("query times : %s %s PreparedStatement's hascode %s".formatted(i,
                    resultSet.getString(1),ps.hashCode()));
            ps.close();
        }
        /**
         * query times : 0 12:12:12.123456 PreparedStatement's hascode 195381554
         * query times : 1 12:12:12.123456 PreparedStatement's hascode 360207322
         * query times : 2 12:12:12.123456 PreparedStatement's hascode 119290689
         * query times : 3 12:12:12.123456 PreparedStatement's hascode 594427726
         * query times : 4 12:12:12.123456 PreparedStatement's hascode 1810899357
         * query times : 5 12:12:12.123 PreparedStatement's hascode 142247393
         * query times : 6 12:12:12.123 PreparedStatement's hascode 1729779847
         */
    }


    @Test
    void test3() throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        statement.execute("""
        drop  table if exists time_table;
        create table time_table (time_type time(6))
        """);
        statement.execute("insert into time_table (time_type) values ('12:12:12.123456')");
        // use one PreparedStatement for seven times query;
        PreparedStatement ps = connection.prepareStatement("select * from time_table");

        for (int i = 0; i < 7; i++) {
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            System.out.println("query times : %s %s PreparedStatement's hascode %s".formatted(i,
                    resultSet.getString(1),ps.hashCode()));

        }
        /**
         * query times : 0 12:12:12.123456 PreparedStatement's hascode 195381554
         * query times : 1 12:12:12.123456 PreparedStatement's hascode 360207322
         * query times : 2 12:12:12.123456 PreparedStatement's hascode 119290689
         * query times : 3 12:12:12.123456 PreparedStatement's hascode 594427726
         * query times : 4 12:12:12.123456 PreparedStatement's hascode 1810899357
         * query times : 5 12:12:12.123 PreparedStatement's hascode 142247393
         * query times : 6 12:12:12.123 PreparedStatement's hascode 1729779847
         */
    }


    /**
     * pgSimpleDataSource.setProperty("preferQueryMode", "simple");
     * @throws SQLException
     */
    @Test
    void test4() throws SQLException {
        Connection connection = getConnectionSimpleMode();
        Statement statement = connection.createStatement();
        statement.execute("""
        drop  table if exists time_table;
        create table time_table (time_type time(6))
        """);
        statement.execute("insert into time_table (time_type) values ('12:12:12.123456')");
        // use one PreparedStatement for seven times query;
        PreparedStatement ps = connection.prepareStatement("select * from time_table");

        for (int i = 0; i < 7; i++) {
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            System.out.println("query times : %s %s PreparedStatement's hascode %s".formatted(i,
                    resultSet.getString(1),ps.hashCode()));

        }
        /**
         * query times : 0 12:12:12.123456 PreparedStatement's hascode 142247393
         * query times : 1 12:12:12.123456 PreparedStatement's hascode 142247393
         * query times : 2 12:12:12.123456 PreparedStatement's hascode 142247393
         * query times : 3 12:12:12.123456 PreparedStatement's hascode 142247393
         * query times : 4 12:12:12.123456 PreparedStatement's hascode 142247393
         * query times : 5 12:12:12.123456 PreparedStatement's hascode 142247393
         * query times : 6 12:12:12.123456 PreparedStatement's hascode 142247393
         */
    }

    /**
     * setPrepareThreshold
     *
     * @throws SQLException
     */
    @Test
    void test5() throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        statement.execute("""
        drop  table if exists time_table;
        create table time_table (time_type time(6))
        """);
        statement.execute("insert into time_table (time_type) values ('12:12:12.123456')");
        // use one PreparedStatement for seven times query;
        PreparedStatement ps = connection.prepareStatement("select * from time_table");
        PGStatement unwrap = ps.unwrap(PGStatement.class);
        // I setPrepareThreshold with 6 so, the seventh time-value will be 12:12:12.123,but sixth not
        unwrap.setPrepareThreshold(6);

        for (int i = 0; i < 7; i++) {
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            System.out.println("query times : %s %s PreparedStatement's hascode %s".formatted(i,
                    resultSet.getString(1),ps.hashCode()));

        }
        /**
         * query times : 0 12:12:12.123456 PreparedStatement's hascode 960733886
         * query times : 1 12:12:12.123456 PreparedStatement's hascode 960733886
         * query times : 2 12:12:12.123456 PreparedStatement's hascode 960733886
         * query times : 3 12:12:12.123456 PreparedStatement's hascode 960733886
         * query times : 4 12:12:12.123456 PreparedStatement's hascode 960733886
         * query times : 5 12:12:12.123456 PreparedStatement's hascode 960733886
         * query times : 6 12:12:12.123 PreparedStatement's hascode 960733886
         */
    }
}
