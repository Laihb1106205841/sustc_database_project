package io.sustc.DatabaseConnection;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;

@Service
@Slf4j
public class SQLDataSource implements Closeable {
    private static final SQLDataSource INSTANCE = new SQLDataSource();

    private HikariDataSource dataSource = new HikariDataSource();

    public static SQLDataSource getInstance() {
        return INSTANCE;
    }
    public SQLDataSource(String jdbcUrl, String username,int ThreadNum) {
        configureSQLServer(
                jdbcUrl,
                username,
                "123456" //,ThreadNum
        );
    }
    public SQLDataSource() {
        configureSQLServer(
                "jdbc:postgresql://localhost:5432/sustc",
                "bilibili",
                "123456"
        );
    }

    public SQLDataSource(int ThreadNum) {
        configureSQLServer(
                "jdbc:postgresql://localhost:5432/sustc",
                "bilibili",
                "123456" //,ThreadNum
        );
    }

    public void configureSQLServer(String jdbcUrl, String username, String password) {
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(16);

        try {
            dataSource.setLoginTimeout(400000);
            dataSource.setConnectionTimeout(400000);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void configureSQLServer(String jdbcUrl, String username, String password,int Thread) {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(Thread);
        log.info("dataSource Pool Size:"+Thread);

        try {
            dataSource.setLoginTimeout(1200000);
            dataSource.setConnectionTimeout(1200000);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getSQLConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
