package io.seqera.events


import java.time.LocalDateTime
import java.time.ZoneOffset

class Emitter {

    static void main(String[] args) {
        Long now = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
        Long oneDayAgo = LocalDateTime.now().minusDays(1).toEpochSecond(ZoneOffset.UTC);
        Double delta = 0.1
        def dataset = []
        while(oneDayAgo<=now){
            delta += 1 / Math.pow((Math.pow(delta,2)) + 1.5 , 2)
            dataset.push(delta)
        }
        Renderer renderer = new Renderer(dataset)
        renderer.chart()


    }
}
