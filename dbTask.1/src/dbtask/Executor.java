package dbtask;

import java.sql.*;

public class Executor {
    public static <T> T execQuery(Connection connection, String query, TResultHandler<T> handler) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
            ResultSet result = stmt.getResultSet();
            T value = handler.handle(result);
            result.close();
            return value;
        }
    }

    public static void execUpdate(Connection connection, String update) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(update);
        }
    }

    public static void execUpdate(Connection connection, String sql, Object... row) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            int pos = 1;
            for (Object value : row) {
                stmt.setObject(pos++, value);
            }
            stmt.executeUpdate();
        }
    }
}
