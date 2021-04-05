package net.intelie.challenges;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class EventTest {
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    @Test
    public void insertionOrder() throws Exception {
        //The ConcurrentSkipListSet will insert in the order that was given by the CompareTo method implemented in the Event Class.
        //In this test it's checked if the order is right (ascending order).
        EventStore eventStore = new EventStoreImpl();

        //It's created 5 events, and then inserted in a non ordered way.
        String type1 = "type1";

        long timestamp1 = 10L;
        long timestamp2 = 20L;
        long timestamp3 = 30L;
        long timestamp4 = 40L;
        long timestamp5 = 50L;
        long timestamp6 = 60L;

        Event event1 = new Event(type1, timestamp1);
        Event event2 = new Event(type1, timestamp2);
        Event event3 = new Event(type1, timestamp3);
        Event event4 = new Event(type1, timestamp4);
        Event event5 = new Event(type1, timestamp5);

        eventStore.insert(event1);
        eventStore.insert(event4);
        eventStore.insert(event5);
        eventStore.insert(event3);
        eventStore.insert(event2);

        //And then a iterator is created to check if the order is right.
        EventIterator eventIterator = eventStore.query(type1, timestamp1, timestamp6);

        assertTrue(eventIterator.moveNext());
        assertEquals(event1, eventIterator.current());

        assertTrue(eventIterator.moveNext());
        assertEquals(event2, eventIterator.current());

        assertTrue(eventIterator.moveNext());
        assertEquals(event3, eventIterator.current());

        assertTrue(eventIterator.moveNext());
        assertEquals(event4, eventIterator.current());

        assertTrue(eventIterator.moveNext());
        assertEquals(event5, eventIterator.current());

        assertFalse(eventIterator.moveNext());
    }

    @Test
    public void removeTest() throws Exception {
        //This test is used to check if the removeAll method is working as intended
        EventStore eventStore = new EventStoreImpl();

        String type1 = "type1";
        String type2 = "type2";

        long timestamp1 = 10L;
        long timestamp2 = 20L;
        long timestamp3 = 30L;
        long timestamp4 = 40L;
        long timestamp5 = 50L;
        long timestamp6 = 60L;

        //It's created 5 events with 'type1' and then 3 events with 'type2', and them every event is inserted into the EventStore
        Event eventt11 = new Event(type1, timestamp1);
        Event eventt12 = new Event(type1, timestamp2);
        Event eventt13 = new Event(type1, timestamp3);
        Event eventt14 = new Event(type1, timestamp4);
        Event eventt15 = new Event(type1, timestamp5);

        Event eventt21 = new Event(type2, timestamp1);
        Event eventt22 = new Event(type2, timestamp2);
        Event eventt23 = new Event(type2, timestamp3);

        eventStore.insert(eventt11);
        eventStore.insert(eventt12);
        eventStore.insert(eventt13);
        eventStore.insert(eventt14);
        eventStore.insert(eventt15);

        eventStore.insert(eventt21);
        eventStore.insert(eventt22);
        eventStore.insert(eventt23);

        //Right here all events with type1 are removed
        eventStore.removeAll(type1);

        //And right here it's created a iterator for each type. And finally it's checked if there are any event with 'type1'
        EventIterator eventIteratorType1 = eventStore.query(type1, timestamp1, timestamp6);
        EventIterator eventIteratorType2 = eventStore.query(type2, timestamp1, timestamp6);

        assertFalse(eventIteratorType1.moveNext());

        assertTrue(eventIteratorType2.moveNext());
        assertEquals(eventt21, eventIteratorType2.current());

        assertTrue(eventIteratorType2.moveNext());
        assertEquals(eventt22, eventIteratorType2.current());

        assertTrue(eventIteratorType2.moveNext());
        assertEquals(eventt23, eventIteratorType2.current());

        assertFalse(eventIteratorType2.moveNext());
    }

    @Test
    public void testInsertWithConcurrency() throws InterruptedException {
        //This test will check whether the problem where multiple threads try to insert the same event
        //The test will use 100 threads
        EventStore eventStore = new EventStoreImpl();
        int numberOfThreads = 100;

        ExecutorService service = Executors.newFixedThreadPool(100);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        //For each thread, there will be a insertion of a unique event, and then every thread will try to insert the same event.
        //The unique event will have either type equals 'type1' or 'type2'
        //The expected behavior is to have exactly numberOfThreads + 1 events
        //This behavior is expected because ConcurrentSkipListSet can't have any duplicate Event
        for (int i = 0; i < numberOfThreads; i++) {
            int finalI = i;
            service.execute(() -> {
                //Inserting a unique event
                int typeNumber = (finalI%2)+1;
                String type = "type".concat(Integer.toString(typeNumber));
                Event uniqueEvent = new Event(type, finalI*10);
                eventStore.insert(uniqueEvent);

                //inserting a event with same attributes
                Event event = new Event("type1", 10L);
                eventStore.insert(event);
                latch.countDown();
            });
        }

        latch.await();
        int eventNumbers = 0;

        //Right here it's checked how many events were inserted in the EventStore
        EventIterator eventIterator = eventStore.query("type1", Long.MIN_VALUE, Long.MAX_VALUE);
        while(eventIterator.moveNext()) {
            eventNumbers++;
        }


        EventIterator eventIterator2 = eventStore.query("type2", Long.MIN_VALUE, Long.MAX_VALUE);
        while(eventIterator2.moveNext()) {
            eventNumbers++;
        }

        assertEquals(numberOfThreads+1, eventNumbers);
    }

    @Test
    public void testRemoveWithConcurrency() throws InterruptedException {
        //This test will check if the RemoveAll method will remove every event with a type, when used by multiple threads
        //First of all multiple events are created and inserted into the EventStore
        EventStore eventStore = new EventStoreImpl();

        String type1 = "type1";
        String type2 = "type2";

        long timestamp1 = 10L;
        long timestamp2 = 20L;
        long timestamp3 = 30L;
        long timestamp4 = 40L;
        long timestamp5 = 50L;

        Event eventt11 = new Event(type1, timestamp1);
        Event eventt12 = new Event(type1, timestamp2);
        Event eventt13 = new Event(type1, timestamp3);
        Event eventt14 = new Event(type1, timestamp4);
        Event eventt15 = new Event(type1, timestamp5);

        Event eventt21 = new Event(type2, timestamp1);
        Event eventt22 = new Event(type2, timestamp2);
        Event eventt23 = new Event(type2, timestamp3);

        eventStore.insert(eventt11);
        eventStore.insert(eventt12);
        eventStore.insert(eventt13);
        eventStore.insert(eventt14);
        eventStore.insert(eventt15);

        eventStore.insert(eventt21);
        eventStore.insert(eventt22);
        eventStore.insert(eventt23);

        //Right here the threads will try to remove every event with type2
        int numberOfThreads = 100;

        ExecutorService service = Executors.newFixedThreadPool(100);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            service.execute(() -> {
                eventStore.removeAll(type2);
                latch.countDown();
            });
        }

        latch.await();
        int eventNumbers = 0;

        //And then it's checked the total numbers of events
        EventIterator eventIterator = eventStore.query("type1", Long.MIN_VALUE, Long.MAX_VALUE);
        while(eventIterator.moveNext()) {
            eventNumbers++;
        }

        EventIterator eventIterator2 = eventStore.query("type2", Long.MIN_VALUE, Long.MAX_VALUE);
        while(eventIterator2.moveNext()) {
            eventNumbers++;
        }

        assertEquals(eventNumbers, 5);
    }

    @Test
    public void testRemoveIteratorWithConcurrency() throws InterruptedException {
        //This test will check if the iterator will remove correctly when there's multiple threads trying to remove the same event

        EventStore eventStore = new EventStoreImpl();

        String type1 = "type1";

        long timestamp1 = 10L;
        long timestamp2 = 20L;
        long timestamp3 = 30L;
        long timestamp4 = 40L;
        long timestamp5 = 50L;

        Event eventt11 = new Event(type1, timestamp1);
        Event eventt12 = new Event(type1, timestamp2);
        Event eventt13 = new Event(type1, timestamp3);
        Event eventt14 = new Event(type1, timestamp4);
        Event eventt15 = new Event(type1, timestamp5);

        eventStore.insert(eventt11);
        eventStore.insert(eventt12);
        eventStore.insert(eventt13);
        eventStore.insert(eventt14);
        eventStore.insert(eventt15);

        int numberOfThreads = 1000;

        ExecutorService service = Executors.newFixedThreadPool(1000);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        //After the creation of 5 events, each thread will try to remove the event with timestamp = 30L and type = 1
        //This removal is done by creating an iterator, and then removing the event
        for (int i = 0; i < numberOfThreads; i++) {
            service.execute(() -> {
                EventIterator eventIterator = eventStore.query(type1, Long.MIN_VALUE, Long.MAX_VALUE);

                while(eventIterator.moveNext()) {
                    if(eventIterator.current().timestamp() == timestamp3) {
                        eventIterator.remove();
                    }
                }
                latch.countDown();
            });
        }
        latch.await();

        int eventNumbers = 0;

        //And finally, it's checked if there's only 4 events
        EventIterator eventIterator = eventStore.query("type1", Long.MIN_VALUE, Long.MAX_VALUE);
        while(eventIterator.moveNext()) {
            eventNumbers++;
        }
        assertEquals(4, eventNumbers);
    }

    @Test
    public void testInsertWhileIteratingWithConcurrency() throws InterruptedException {
        //This test is checking if the iterator is the same after inserting a Event while iterating
        //The expected behavior is to keep the iterator the same even if there are other threads removing or inserting

        EventStore eventStore = new EventStoreImpl();

        String type1 = "type1";

        long timestamp1 = 10L;
        long timestamp2 = 20L;
        long timestamp3 = 30L;
        long timestamp4 = 40L;
        long timestamp5 = 50L;
        long timestamp6 = 50L;

        Event eventt11 = new Event(type1, timestamp1);
        Event eventt12 = new Event(type1, timestamp2);
        Event eventt13 = new Event(type1, timestamp3);
        Event eventt15 = new Event(type1, timestamp5);
        Event eventt16 = new Event(type1, timestamp6);

        eventStore.insert(eventt11);
        eventStore.insert(eventt12);
        eventStore.insert(eventt13);
        eventStore.insert(eventt15);
        eventStore.insert(eventt16);

        int numberOfThreads = 2;

        ExecutorService service = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        AtomicReference<Integer> totalEvents = new AtomicReference<>(0);

        //This test is using only 2 threads
        //One thread will create the iterator and then wait for the other thread to add a new event
        //After that the thread will iterate through all events gathered and count the total number of events queried
        for (int i = 0; i < numberOfThreads; i++) {
            int threadNumber = i;
            service.execute(() -> {
                if(threadNumber == 0) {
                    EventIterator eventIterator = eventStore.query(type1, Long.MIN_VALUE, Long.MAX_VALUE);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    while(eventIterator.moveNext()) {
                        totalEvents.getAndSet(totalEvents.get() + 1);
                    }
                } else {
                    Event eventt14 = new Event(type1, timestamp4);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    eventStore.insert(eventt14);
                }
                latch.countDown();
            });
        }
        latch.await();
        assertEquals(5, (int) totalEvents.get());
    }

    @Test
    public void testRemoveWhileIteratingWithConcurrency() throws InterruptedException {
        //This test it's checking the same behavior as the test "testInsertWhileIteratingWithConcurrency", but with removal instead of insertion

        EventStore eventStore = new EventStoreImpl();

        String type1 = "type1";

        long timestamp1 = 10L;
        long timestamp2 = 20L;
        long timestamp3 = 30L;
        long timestamp4 = 40L;
        long timestamp5 = 50L;

        Event eventt11 = new Event(type1, timestamp1);
        Event eventt12 = new Event(type1, timestamp2);
        Event eventt13 = new Event(type1, timestamp3);
        Event eventt14 = new Event(type1, timestamp4);
        Event eventt15 = new Event(type1, timestamp5);

        eventStore.insert(eventt11);
        eventStore.insert(eventt12);
        eventStore.insert(eventt13);
        eventStore.insert(eventt14);
        eventStore.insert(eventt15);

        int numberOfThreads = 2;

        ExecutorService service = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        AtomicReference<Integer> totalEvents = new AtomicReference<>(0);

        for (int i = 0; i < numberOfThreads; i++) {
            int threadNumber = i;
            service.execute(() -> {
                if(threadNumber == 0) {
                    EventIterator eventIterator = eventStore.query(type1, Long.MIN_VALUE, Long.MAX_VALUE);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    while(eventIterator.moveNext()) {
                        totalEvents.getAndSet(totalEvents.get() + 1);
                    }
                } else {
                    EventIterator eventIterator = eventStore.query(type1, Long.MIN_VALUE, Long.MAX_VALUE);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    eventIterator.moveNext();
                    eventIterator.remove();
                }

                latch.countDown();
            });
        }
        latch.await();
        assertEquals(5, (int) totalEvents.get());
    }


}