package net.intelie.challenges;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class EventStoreImpl implements EventStore {

    /*I've chosen this data structure because of the following reasons:
        * - This data structure is already thread-safe for insertion, removal and access by multiple threads
        * - This data structure is fast, the average time cost for add and remove is log(n)
        * - This data structure won't lock the EventList, so it's faster than other methods
    * So, as the challenge asked for a thread-safe and fast structure, i've decided to implement it using the ConcurrentSkipListSet.
    * The only problem is that this data structure won't accept duplicate elements or Null Elements.
    * But as the challenge didn't specified that there was the possibility of having multiple events with the same type and timestamp,
    * i've decided that this data structure is a good way to implement it.
    */
    private ConcurrentSkipListSet<Event> eventList;

    public EventStoreImpl() {
        eventList = new ConcurrentSkipListSet<>();
    }

    @Override
    public void insert(Event event) {
        /*
        This insertion is really simple, i only need to use the ordered insertion of the ConcurrentSkipListSet.
         */
        eventList.add(event);
    }

    @Override
    public void removeAll(String type) {
        /*
        To do the removeAll method, i cretated two events with the desired type to remove. One event will have the minimum value possible for a timestamp
        And the other event will have the maximum value for a timestamp. And then i retrieved every event with that type, and then i removed them all using the removeAll
        method from ConcurrentSkipListSet.
         */
        long maxTimestamp = Long.MAX_VALUE;
        long minTimestamp = Long.MIN_VALUE;
        Event minEventTimestamp = new Event(type, minTimestamp);
        Event maxEventTimestamp = new Event(type, maxTimestamp);
        NavigableSet<Event> eventsToRemove = eventList.subSet(minEventTimestamp, true, maxEventTimestamp, true);
        eventList.removeAll(eventsToRemove);
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        /*
        And for this method i used the subset method to retrieve every event
        and then created a iterator from that subset.
         */
        Event startEvent = new Event(type, startTime);
        Event endEvent = new Event(type, endTime);
        Iterator<Event> eventIterator = eventList.subSet(startEvent, endEvent).iterator();

        return new EventIteratorImpl(eventIterator);
    }

}
