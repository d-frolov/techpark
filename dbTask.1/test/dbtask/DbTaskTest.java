package dbtask;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by d.frolov on 12/8/15.
 */

public class DbTaskTest {

    @SuppressWarnings("MagicNumber")
    private static final long[] VALUES = new long[]{30000, 1000000, 465, 81274, 233333};
    private static final int THREADS_COUNT = 11;
    private static final int TRANS_COUNT = 99;
    private static final long TRANS_BOUND = 100000;

    @Before
    public void init() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        Connector.init();

        try (Connection connection = Connector.getConnection()) {
            DbTask.create(connection);
            DbTask.init(connection, VALUES);
        }
    }

    @Test
    public void test() throws SQLException {
        long initSum;
        try (Connection connection = Connector.getConnection()) {
            initSum = DbTask.getSum(connection);
        }

        runTransfers();

        check(initSum);
    }

    private void runTransfers() {
        Thread[] threads = new Thread[THREADS_COUNT];
        for (int i = 0; i < THREADS_COUNT; i++)
            threads[i] = new Thread(new TransRunnable(TRANS_COUNT), "t" + i);

        for (Thread thread : threads)
            thread.start();

        for (Thread thread : threads)
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        System.out.println("All threads finished!");
    }

    static class TransRunnable implements Runnable {
        private final int transCount;

        TransRunnable(int transCount) {
            this.transCount = transCount;
        }

        @Override
        public void run() {
            try (Connection connection = Connector.getConnection()) {
                for (int i = 0; i < transCount; i++) {
                    int from = ThreadLocalRandom.current().nextInt(VALUES.length);
                    int to = ThreadLocalRandom.current().nextInt(VALUES.length);
                    long value = ThreadLocalRandom.current().nextLong(TRANS_BOUND) + 1;
                    try {
                        DbTask.transfer(connection, from, to, value);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void check(long initSum) throws SQLException {
        try (Connection connection = Connector.getConnection()) {
            Assert.assertEquals("Sums not equals!", initSum, DbTask.getSum(connection));

            int size = Executor.execQuery(connection, "select count(*) from account", resultSet -> {
                resultSet.next();
                return resultSet.getInt(1);
            });
            Assert.assertEquals("Accounts count not equals!", VALUES.length, size);

            long[] values = Executor.execQuery(connection, "select value from account order by id", resultSet -> {
                long[] vals = new long[VALUES.length];
                int i = 0;
                while (resultSet.next()) {
                    vals[i++] = resultSet.getLong(1);
                }
                return vals;
            });
            for (long value : values)
                Assert.assertTrue("Some values are negative!", value >= 0);
        }
    }

    @After
    public void cleanup() throws SQLException {
        try (Connection connection = Connector.getConnection()) {
            DbTask.drop(connection);
        }
    }
}


