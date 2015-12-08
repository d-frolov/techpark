package dbtask;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by d.frolov on 12/8/15.
 */

public class DbTask {

    public static void create(@NotNull Connection connection) throws SQLException {
        Executor.execUpdate(connection, "create table account (id integer, value long)");
    }

    public static void drop(@NotNull Connection connection) throws SQLException {
        Executor.execUpdate(connection, "drop table account");
    }

    public static void init(@NotNull Connection connection, long[] values) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            Executor.execUpdate(connection, "insert into account (id, value) values (?,?)", i, values[i]);
        }
    }

    public static long getSum(@NotNull Connection connection) throws SQLException {
        return Executor.execQuery(connection, "select sum(value) from account", resultSet -> {
            resultSet.next();
            return resultSet.getLong(1);
        });
    }

    /*  Надо переписать вот эту функцию, чтоб она корректно списывала и зачисляла средства на счета. Счета не могу быть отрицательными */
    public static void transfer(@NotNull Connection connection, int from, int to, long value) throws SQLException {
      if(from != to) {
          Executor.execUpdate(connection, "update account set value = value - ? where id = ?", value, from);
          Executor.execUpdate(connection, "update account set value = value + ? where id = ?", value, to);
      }
    }
}
