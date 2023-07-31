package io.seqera.events.utils.db

import groovy.sql.Sql

import javax.sql.ConnectionEvent
import javax.sql.ConnectionEventListener
import javax.sql.DataSource
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.logging.Logger

class PooledDataSource implements DataSource, ConnectionEventListener {

    static int DEFAULT_LOGIN_TIMEOUT = 300
    static int DEFAULT_IDLE_TIMEOUT = 0

    static int DEFAULT_INITIAL_POOL_SIZE = 10

    private int loginTimeout = DEFAULT_LOGIN_TIMEOUT
    private int idleTimeout = DEFAULT_IDLE_TIMEOUT

    private int initialPoolSize = DEFAULT_INITIAL_POOL_SIZE

    private String serverUrl
    private String username
    private String password

    private def connections = new PooledConnectionImpl[initialPoolSize]

    private PrintWriter logWriter = System.out.newPrintWriter()

    PooledDataSource(
            String serverUrl,
            String username,
            String password,
            String driver,
            int loginTimeout = DEFAULT_LOGIN_TIMEOUT,
            int idleTimeout = DEFAULT_IDLE_TIMEOUT,
            int initialPoolSize = DEFAULT_INITIAL_POOL_SIZE
    ) {

        assert loginTimeout >= 0
        assert idleTimeout >= 0
        assert initialPoolSize > 0

        Sql.loadDriver(driver)
        this.serverUrl = serverUrl
        this.username = username
        this.password = password
        this.loginTimeout = loginTimeout
        this.idleTimeout = idleTimeout
        this.initialPoolSize = initialPoolSize

        allocateConnections(initialPoolSize, serverUrl, username, password)
    }

    @Override
    Connection getConnection() throws SQLException {
        return new ConnectionImpl(connections[0]) // TODO
    }

    @Override
    Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException()
    }

    @Override
    void connectionClosed(ConnectionEvent event) {
        // TODO Re-allocate resources
    }

    @Override
    void connectionErrorOccurred(ConnectionEvent event) {

    }

    @Override
    PrintWriter getLogWriter() throws SQLException {
        return logWriter
    }

    @Override
    void setLogWriter(PrintWriter out) throws SQLException {
        this.logWriter = out
    }

    @Override
    void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds
    }

    @Override
    int getLoginTimeout() throws SQLException {
        return loginTimeout
    }

    long getIdleTimeout() {
        return idleTimeout
    }

    int getInitialPoolSize() {
        return initialPoolSize
    }

    @Override
    Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException()
    }

    @Override
    <T> T unwrap(Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException()
    }

    @Override
    boolean isWrapperFor(Class<?> type) throws SQLException {
        return false
    }

    private void allocateConnections(int initialPoolSize, String serverUrl, String username, String password) {
        for (i in 0..initialPoolSize - 1) {
            def connection = DriverManager.getConnection(serverUrl, username, password)
            def pooledConnection = new PooledConnectionImpl(connection)
            pooledConnection.addConnectionEventListener(this)
            connections[i] = pooledConnection
        }
    }
}
