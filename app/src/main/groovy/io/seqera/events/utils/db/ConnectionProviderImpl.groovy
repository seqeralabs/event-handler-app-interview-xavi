package io.seqera.events.utils.db

import groovy.sql.Sql
import groovy.transform.TupleConstructor

@TupleConstructor
class ConnectionProviderImpl implements ConnectionProvider {

    String serverUrl
    String username
    String password
    String driver

    int idleTimeoutSeconds
    int initialPoolSize

    @Override
    Sql getConnection() {
        def pool = new PooledDataSource(serverUrl, username, password, driver, idleTimeoutSeconds, initialPoolSize)
        return new Sql(pool)
    }
}
