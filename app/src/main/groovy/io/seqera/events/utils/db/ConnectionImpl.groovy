package io.seqera.events.utils.db

import java.sql.Connection

class ConnectionImpl {

    private PooledConnectionImpl pooledConnection

    @Delegate
    private Connection connection

    ConnectionImpl(PooledConnectionImpl pooledConnection) {
        this.pooledConnection = pooledConnection
        this.connection = pooledConnection.getConnection()
    }

    void close() {
        pooledConnection.release()
    }
}
