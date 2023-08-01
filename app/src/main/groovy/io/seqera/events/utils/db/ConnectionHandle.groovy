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
import java.time.Clock
import java.util.concurrent.Executor

@CompileStatic
class ConnectionHandle implements Connection {

    private PooledConnectionImpl pooledConnection
    private Connection connection

    protected long lastUsed

    ConnectionHandle(PooledConnectionImpl pooledConnection, Connection connection) {
        this.pooledConnection = pooledConnection
        this.connection = connection
    }

    @Override
    Statement createStatement() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createStatement()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareStatement(sql)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    CallableStatement prepareCall(String sql) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareCall(sql)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    String nativeSQL(String sql) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.nativeSQL(sql)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setAutoCommit(boolean autoCommit) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setAutoCommit(autoCommit)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    boolean getAutoCommit() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getAutoCommit()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void commit() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.commit()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void rollback() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.rollback()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void close() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            pooledConnection.notifyConnectionClosed()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    boolean isClosed() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.isClosed()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    DatabaseMetaData getMetaData() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getMetaData()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setReadOnly(boolean readOnly) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setReadOnly(readOnly)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    boolean isReadOnly() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.isReadOnly()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setCatalog(String catalog) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setCatalog(catalog)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    String getCatalog() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getCatalog()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setTransactionIsolation(int level) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setTransactionIsolation(level)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    int getTransactionIsolation() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getTransactionIsolation()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    SQLWarning getWarnings() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getWarnings()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void clearWarnings() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.clearWarnings()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createStatement(resultSetType, resultSetConcurrency)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareStatement(sql, resultSetType, resultSetConcurrency)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareCall(sql, resultSetType, resultSetConcurrency)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Map<String, Class<?>> getTypeMap() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getTypeMap()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setTypeMap(map)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setHoldability(int holdability) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setHoldability(holdability)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    int getHoldability() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getHoldability()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Savepoint setSavepoint() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.setSavepoint()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Savepoint setSavepoint(String name) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.setSavepoint(name)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void rollback(Savepoint savepoint) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.rollback(savepoint)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void releaseSavepoint(Savepoint savepoint) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.releaseSavepoint(savepoint)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareStatement(sql, autoGeneratedKeys)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareStatement(sql, columnIndexes)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.prepareStatement(sql, columnNames)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Clob createClob() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createClob()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Blob createBlob() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createBlob()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    NClob createNClob() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createNClob()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    SQLXML createSQLXML() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createSQLXML()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    boolean isValid(int timeout) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.isValid(timeout)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setClientInfo(String name, String value) throws SQLClientInfoException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setClientInfo(name, value)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setClientInfo(Properties properties) throws SQLClientInfoException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setClientInfo(properties)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    String getClientInfo(String name) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getClientInfo(name)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Properties getClientInfo() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getClientInfo()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createArrayOf(typeName, elements)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.createStruct(typeName, attributes)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setSchema(String schema) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setSchema(schema)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    String getSchema() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getSchema()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void abort(Executor executor) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.abort(executor)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setNetworkTimeout(executor, milliseconds)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    int getNetworkTimeout() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.getNetworkTimeout()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void beginRequest() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.beginRequest()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void endRequest() throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.endRequest()
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.setShardingKeyIfValid(shardingKey, superShardingKey, timeout)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            return connection.setShardingKeyIfValid(shardingKey, timeout)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setShardingKey(shardingKey, superShardingKey)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
        }
    }

    @Override
    void setShardingKey(ShardingKey shardingKey) throws SQLException {
        lastUsed = System.currentTimeMillis()
        try {
            connection.setShardingKey(shardingKey)
        } catch (SQLNonTransientConnectionException e) {
            throw handleConnectionError(e)
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

    private def handleConnectionError(SQLNonTransientConnectionException exception) {
        pooledConnection.notifyConnectionError(exception)
        return exception
    }
}
