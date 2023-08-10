package io.seqera.events.utils.db

import groovy.transform.CompileStatic
import java.sql.Connection
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import javax.sql.ConnectionEvent
import javax.sql.ConnectionEventListener
import javax.sql.PooledConnection
import javax.sql.StatementEventListener

@CompileStatic
class PooledConnectionImpl implements PooledConnection {

    private Connection connection
    private ConnectionHandle handle

    private Set<ConnectionEventListener> listeners = new HashSet<>()

    PooledConnectionImpl(Connection connection) {
        this.connection = connection
    }

    @Override
    Connection getConnection() throws SQLException {
        if (handle == null) {
            handle = new ConnectionHandle(this, connection)
        }
        return handle
    }

    @Override
    void close() throws SQLException {
        connection.close()
    }

    @Override
    void addConnectionEventListener(ConnectionEventListener listener) {
        listeners.add(listener)
    }

    @Override
    void removeConnectionEventListener(ConnectionEventListener listener) {
        listeners.remove(listeners)
    }

    @Override
    void addStatementEventListener(StatementEventListener listener) {
        throw new SQLFeatureNotSupportedException()
    }

    @Override
    void removeStatementEventListener(StatementEventListener listener) {
        throw new SQLFeatureNotSupportedException()
    }

    protected void notifyConnectionClosed() {
        def event = new ConnectionEvent(this)
        for (listener in listeners) listener.connectionClosed(event)
        handle = null
    }

    protected void notifyConnectionError(SQLException exception) {
        def event = new ConnectionEvent(this, exception)
        for (listener in listeners) listener.connectionErrorOccurred(event)
    }
}
