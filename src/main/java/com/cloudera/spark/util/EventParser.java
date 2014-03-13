package com.cloudera.spark.util;

import com.cloudera.spark.common.STBEvent;

/**
 * Created by spabba on 3/13/14.
 */
public class EventParser {

    public static STBEvent createEvent(String eventStr) throws Exception {

        String[] eventAttributes = eventStr.split(",");

        STBEvent event = new STBEvent();
        event.setSubscriberId(eventAttributes[0]);
        event.setEventDt(Long.parseLong(eventAttributes[1]));
        event.setEventType(eventAttributes[2]);
        event.setEventData(eventAttributes[3]);


        return event;
    }
}
