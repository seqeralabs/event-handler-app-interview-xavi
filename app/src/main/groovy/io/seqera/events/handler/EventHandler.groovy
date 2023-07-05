package io.seqera.events.handler

import com.sun.net.httpserver.HttpExchange
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import io.seqera.events.dao.EventDao
import groovy.json.JsonSlurper
import io.seqera.events.dto.Event

@CompileStatic
class EventHandler implements Handler {

    private EventDao eventDao
    private JsonSlurper json

    EventHandler(EventDao dao){
        this.eventDao = dao
        this.json = new JsonSlurper()
    }

    @Override
    String getHandlerPath() {
        return "/events"
    }

    @Override
    void handle(HttpExchange http) throws IOException {
        if(http.requestMethod != "POST") {
            http.sendResponseHeaders(405, 0)
            return
        }
        def body = http.requestBody.text
        def event = this.json.parseText(body) as Event
        event = this.eventDao.save(event)
        // TODO: encapsulate common flow into super class handling json header and parsing
        http.responseHeaders.add("Content-type", "application/json")
        def response = JsonOutput.toJson(event)
        http.sendResponseHeaders(200, response.length())
        http.responseBody.withWriter { out ->
            out << response
        }
    }
}
