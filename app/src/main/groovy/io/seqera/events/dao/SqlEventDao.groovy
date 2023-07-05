package io.seqera.events.dao

import groovy.sql.Sql
import groovy.transform.CompileStatic
import io.seqera.events.dto.Event

@CompileStatic
class SqlEventDao implements EventDao {

    private Sql sql;
    def String tableName = "EVENT"

    public SqlEventDao(Sql sql){
        this.sql = sql;
    }

    @Override
    Event save(Event event) {
        String query = """insert into ${tableName}(workspaceId, userId, cpu, mem,io) values ('$event.workspaceId','$event.userId',$event.cpu,$event.mem,$event.io)"""
        def id = sql.executeInsert(query)[0][0] as Long
        event.id = id
        return event
    }
}
