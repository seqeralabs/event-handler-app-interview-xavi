package io.seqera.events.utils.db

import groovy.transform.CompileStatic
import java.sql.Array
import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.NClob
import java.sql.PreparedStatement
import java.sql.SQLClientInfoException
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLNonTransientConnectionException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.ShardingKey
import java.sql.Statement
import java.sql.Struct
import java.util.concurrent.Executor

@CompileStatic
class ConnectionHandle implements Connection {

    private PooledConnectionImpl pooledConnection
    private Connection connection

    private long lastUsed

    boolean closed = false

    ConnectionHandle(PooledConnectionImpl pooledConnection, Connection connection) {
        this.pooledConnection = pooledConnection
        this.connection = connection
    }

    @Override
    Statement createStatement() throws SQLException {
        return run {
            connection.createStatement()
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql) throws SQLException {
        return run {
            connection.prepareStatement(sql)
        }
    }

    @Override
    CallableStatement prepareCall(String sql) throws SQLException {
        return run {
            connection.prepareCall(sql)
        }
    }

    @Override
    String nativeSQL(String sql) throws SQLException {
        return run {
            connection.nativeSQL(sql)
        }
    }

    @Override
    void setAutoCommit(boolean autoCommit) throws SQLException {
        run {
            connection.setAutoCommit(autoCommit)
        }
    }

    @Override
    boolean getAutoCommit() throws SQLException {
        return run {
            connection.getAutoCommit()
        }
    }

    @Override
    void commit() throws SQLException {
        run {
            connection.commit()
        }
    }

    @Override
    void rollback() throws SQLException {
        run {
            connection.rollback()
        }
    }

    @Override
    void close() throws SQLException {
        closed = true
        pooledConnection.notifyConnectionClosed()
    }

    @Override
    boolean isClosed() throws SQLException {
        closed
    }

    @Override
    DatabaseMetaData getMetaData() throws SQLException {
        return run {
            connection.getMetaData()
        }
    }

    @Override
    void setReadOnly(boolean readOnly) throws SQLException {
        run {
            connection.setReadOnly(readOnly)
        }
    }

    @Override
    boolean isReadOnly() throws SQLException {
        return run {
            connection.isReadOnly()
        }
    }

    @Override
    void setCatalog(String catalog) throws SQLException {
        run {
            connection.setCatalog(catalog)
        }
    }

    @Override
    String getCatalog() throws SQLException {
        return run {
            connection.getCatalog()
        }
    }

    @Override
    void setTransactionIsolation(int level) throws SQLException {
        run {
            connection.setTransactionIsolation(level)
        }
    }

    @Override
    int getTransactionIsolation() throws SQLException {
        return run {
            connection.getTransactionIsolation()
        }
    }

    @Override
    SQLWarning getWarnings() throws SQLException {
        return run {
            connection.getWarnings()
        }
    }

    @Override
    void clearWarnings() throws SQLException {
        run {
            connection.clearWarnings()
        }
    }

    @Override
    Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return run {
            connection.createStatement(resultSetType, resultSetConcurrency)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return run {
            connection.prepareStatement(sql, resultSetType, resultSetConcurrency)
        }
    }

    @Override
    CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return run {
            connection.prepareCall(sql, resultSetType, resultSetConcurrency)
        }
    }

    @Override
    Map<String, Class<?>> getTypeMap() throws SQLException {
        return run {
            connection.getTypeMap()
        }
    }

    @Override
    void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        run {
            connection.setTypeMap(map)
        }
    }

    @Override
    void setHoldability(int holdability) throws SQLException {
        run {
            connection.setHoldability(holdability)
        }
    }

    @Override
    int getHoldability() throws SQLException {
        return run {
            connection.getHoldability()
        }
    }

    @Override
    Savepoint setSavepoint() throws SQLException {
        return run {
            connection.setSavepoint()
        }
    }

    @Override
    Savepoint setSavepoint(String name) throws SQLException {
        return run {
            connection.setSavepoint(name)
        }
    }

    @Override
    void rollback(Savepoint savepoint) throws SQLException {
        run {
            connection.rollback(savepoint)
        }
    }

    @Override
    void releaseSavepoint(Savepoint savepoint) throws SQLException {
        run {
            connection.releaseSavepoint(savepoint)
        }
    }

    @Override
    Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return run {
            connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return run {
            connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
        }
    }

    @Override
    CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return run {
            connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return run {
            connection.prepareStatement(sql, autoGeneratedKeys)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return run {
            connection.prepareStatement(sql, columnIndexes)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return run {
            connection.prepareStatement(sql, columnNames)
        }
    }

    @Override
    Clob createClob() throws SQLException {
        return run {
            connection.createClob()
        }
    }

    @Override
    Blob createBlob() throws SQLException {
        return run {
            connection.createBlob()
        }
    }

    @Override
    NClob createNClob() throws SQLException {
        return run {
            connection.createNClob()
        }
    }

    @Override
    SQLXML createSQLXML() throws SQLException {
        return run {
            connection.createSQLXML()
        }
    }

    @Override
    boolean isValid(int timeout) throws SQLException {
        return run {
            connection.isValid(timeout)
        }
    }

    @Override
    void setClientInfo(String name, String value) throws SQLClientInfoException {
        run {
            connection.setClientInfo(name, value)
        }
    }

    @Override
    void setClientInfo(Properties properties) throws SQLClientInfoException {
        run {
            connection.setClientInfo(properties)
        }
    }

    @Override
    String getClientInfo(String name) throws SQLException {
        return run {
            connection.getClientInfo(name)
        }
    }

    @Override
    Properties getClientInfo() throws SQLException {
        return run {
            connection.getClientInfo()
        }
    }

    @Override
    Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return run {
            connection.createArrayOf(typeName, elements)
        }
    }

    @Override
    Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return run {
            connection.createStruct(typeName, attributes)
        }
    }

    @Override
    void setSchema(String schema) throws SQLException {
        run {
            connection.setSchema(schema)
        }
    }

    @Override
    String getSchema() throws SQLException {
        return run {
            connection.getSchema()
        }
    }

    @Override
    void abort(Executor executor) throws SQLException {
        run {
            connection.abort(executor)
        }
    }

    @Override
    void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        run {
            connection.setNetworkTimeout(executor, milliseconds)
        }
    }

    @Override
    int getNetworkTimeout() throws SQLException {
        return run {
            connection.getNetworkTimeout()
        }
    }

    @Override
    void beginRequest() throws SQLException {
        run {
            connection.beginRequest()
        }
    }

    @Override
    void endRequest() throws SQLException {
        run {
            connection.endRequest()
        }
    }

    @Override
    boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
        return run {
            connection.setShardingKeyIfValid(shardingKey, superShardingKey, timeout)
        }
    }

    @Override
    boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        return run {
            connection.setShardingKeyIfValid(shardingKey, timeout)
        }
    }

    @Override
    void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        run {
            connection.setShardingKey(shardingKey, superShardingKey)
        }
    }

    @Override
    void setShardingKey(ShardingKey shardingKey) throws SQLException {
        run {
            connection.setShardingKey(shardingKey)
        }
    }

    @Override
    <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException()
    }

    @Override
    boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false
    }

    protected long getLastUsed() {
        lastUsed
    }

    private <T> T run(Closure closure) {
        if (closed) {
            throw new SQLNonTransientConnectionException("Connection is closed")
        }
        lastUsed = System.currentTimeMillis()
        try {
            return closure.call() as T
        } catch (SQLNonTransientConnectionException e) {
            pooledConnection.notifyConnectionError(e)
            throw e
        }
    }
}
