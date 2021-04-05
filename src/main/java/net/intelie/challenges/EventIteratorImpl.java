package net.intelie.challenges;

import java.util.Iterator;

public class EventIteratorImpl implements EventIterator {
    /*
    For the iterator, i only used two attributes:
        - A Iterator of Events
        - A event pointing for the current event
    This implementation have some pros and cons.
    The pros is that is really simple to implement, and wont have problems trying to remove events that were removed by other threads.
    The cons is that if the iterator retrieved from the ConcurrentSkipListSet is changed (removal or insertion of a Event),
    this change won't be reflected to the iterator.
     */

    Iterator<Event> eventIterator;
    Event currentEvent;

    public EventIteratorImpl(Iterator<Event> eventIterator) {
        this.eventIterator = eventIterator;
        currentEvent = null;
    }

    @Override
    public boolean moveNext() {
        //this method only need to check if there is a next event or not, and update the currentEvent and return true or false
        if(eventIterator.hasNext()) {
            currentEvent = eventIterator.next();
            return true;
        } else {
            currentEvent = null;
            return false;
        }
    }

    @Override
    public Event current() {
        //this is anothe really simple method, i just need to return the currentEvent variable
        if(currentEvent == null)
            throw new IllegalStateException();
        return currentEvent;
    }


    @Override
    public void remove() throws IllegalStateException {
        //and another really simple method... To remove the event from the iterator, i just need to call the remove method
        //I just check if currentEvent is null and then remove
        if(currentEvent != null) {
            eventIterator.remove();
            currentEvent = null;
        //CurrentEvent can be null if the moveNext method was never called or if the iterator is empty.
        } else
            throw new IllegalStateException();
    }


    @Override
    public void close() throws Exception {
        //there is no need to close any resource, so this method is empty
    }
}
