package io.seqera.events.utils.db

import groovy.sql.Sql
import groovy.transform.CompileStatic
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.concurrent.atomic.AtomicReferenceArray
import java.util.logging.Logger
import javax.sql.ConnectionEvent
import javax.sql.ConnectionEventListener
import javax.sql.DataSource

import static io.seqera.events.utils.db.PooledDataSource.ConnectionState.ALLOCATED
import static io.seqera.events.utils.db.PooledDataSource.ConnectionState.AVAILABLE
import static io.seqera.events.utils.db.PooledDataSource.ConnectionState.EMPTY

@CompileStatic
class PooledDataSource implements DataSource, ConnectionEventListener {

    static int DEFAULT_IDLE_TIMEOUT_SECONDS = 5
    static int DEFAULT_INITIAL_POOL_SIZE = 10

    private int idleTimeout = DEFAULT_IDLE_TIMEOUT_SECONDS
    private int initialPoolSize = DEFAULT_INITIAL_POOL_SIZE

    private PooledConnectionImpl[] connections
    private AtomicReferenceArray<ConnectionState> states

    private PrintWriter logWriter = System.out.newPrintWriter()

    private String serverUrl
    private String username
    private String password

    private Timer timer

    private enum ConnectionState {
        EMPTY, AVAILABLE, ALLOCATED
    }

    PooledDataSource(
            String serverUrl,
            String username,
            String password,
            String driver,
            int idleTimeout = DEFAULT_IDLE_TIMEOUT_SECONDS,
            int initialPoolSize = DEFAULT_INITIAL_POOL_SIZE
    ) {
        assert idleTimeout >= 0
        assert initialPoolSize > 0

        Sql.loadDriver(driver)
        this.serverUrl = serverUrl
        this.username = username
        this.password = password
        this.idleTimeout = idleTimeout
        this.initialPoolSize = initialPoolSize

        connections = new PooledConnectionImpl[initialPoolSize]
        states = new AtomicReferenceArray<ConnectionState>(initialPoolSize)

        // Create initial pool of connections
        for (int i = 0; i < connections.length; i++) {
            connections[i] = createConnection()
            states.set(i, AVAILABLE)
        }

        if (0 < idleTimeout) {
            this.timer = new Timer()
            this.timer.schedule({
                checkConnectionTimeouts()
            } as TimerTask, 1000, 1000)
        }
    }

    @Override
    Connection getConnection() throws SQLException {
        for (int i = 0; i < states.length(); i++) {
            if (states.compareAndSet(i, AVAILABLE, ALLOCATED)) {
                return connections[i].connection
            }
            if (states.compareAndSet(i, EMPTY, ALLOCATED)) {
                try {
                    connections[i] = createConnection()
                    return connections[i].connection
                } catch (SQLException e) {
                    e.printStackTrace()
                    states.set(i, EMPTY)
                }
            }
        }
        throw new SQLException("No connection available")
    }

    @Override
    Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException()
    }

    @Override
    void connectionClosed(ConnectionEvent event) {
        for (int i = 0; i < connections.length; i++) {
            if (connections[i] == event.source) {
                states.set(i, AVAILABLE)
                break
            }
        }
    }

    @Override
    void connectionErrorOccurred(ConnectionEvent event) {
        for (int i = 0; i < connections.length; i++) {
            if (connections[i] == event.source) {
                states.set(i, ALLOCATED)
                connections[i] = null
                states.set(i, EMPTY)
                break
            }
        }
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
        throw new SQLFeatureNotSupportedException()
    }

    @Override
    int getLoginTimeout() throws SQLException {
        return 0
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

    private PooledConnectionImpl createConnection() {
        def connection = DriverManager.getConnection(serverUrl, username, password)
        def pooledConnection = new PooledConnectionImpl(connection)
        pooledConnection.addConnectionEventListener(this)
        return pooledConnection
    }

    private void checkConnectionTimeouts() {
        def idleTimeoutMillis = idleTimeout * 1000
        for (int i = 0; i < connections.length; i++) {
            if (states.get(i) == ALLOCATED) {
                def handle = connections[i].connection as ConnectionHandle
                if (idleTimeoutMillis < System.currentTimeMillis() - handle.lastUsed) {
                    connectionClosed(new ConnectionEvent(connections[i]))
                }
            }
        }
    }
}
