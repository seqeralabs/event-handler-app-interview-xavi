package io.seqera.events.utils.db

import groovy.transform.CompileStatic

import javax.sql.ConnectionEvent
import javax.sql.ConnectionEventListener
import javax.sql.PooledConnection
import javax.sql.StatementEventListener
import java.sql.Connection
import java.sql.SQLException

@CompileStatic
class PooledConnectionImpl implements PooledConnection {

    private Connection connection

    private Set<ConnectionEventListener> listeners = new HashSet<>()

    PooledConnectionImpl(Connection connection) {
        this.connection = connection
    }

    @Override
    Connection getConnection() throws SQLException {
        return connection
    }

    @Override
    void close() throws SQLException {
        connection.close()
    }

    void release() {
        def event = new ConnectionEvent(this)
        for (listener in listeners) listener.connectionClosed(event)
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
    }

    @Override
    void removeStatementEventListener(StatementEventListener listener) {
    }
}
