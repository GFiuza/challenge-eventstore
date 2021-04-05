package net.intelie.challenges;

public class ChallengeMain {
    public static void main(String[] args) {
        EventStore eventStore = new EventStoreImpl();
        Event e110 = new Event("tipo1", 10);
        Event e130 = new Event("tipo2", 30);
        Event e120 = new Event("tipo1", 20);
        Event e190 = new Event("tipo3", 90);
        Event e150 = new Event("tipo1", 50);
        Event e160 = new Event("tipo3", 60);
        Event e140 = new Event("tipo1", 40);
        Event e210 = new Event("tipo2", 10);
        eventStore.insert(e110);
        eventStore.insert(e130);
        eventStore.insert(e120);
        eventStore.insert(e210);
        eventStore.insert(e190);
        eventStore.insert(e150);
        eventStore.insert(e160);
        eventStore.insert(e140);

        EventIterator eventIterator = eventStore.query("tipo1", 10, 90);
        while(eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.println(event.timestamp());
            eventIterator.remove();
            //eventIterator.remove();
        }
        System.out.println("--------------------------------");

        //eventStore.removeAll("tipo1");

        eventIterator = eventStore.query("tipo1", 10, 100);
        while(eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.println(event.timestamp());
        }
        System.out.println("--------------------------------");

        eventIterator = eventStore.query("tipo2", 10, 90);
        while(eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.println(event.timestamp());
        }

        System.out.println("--------------------------------");

        eventIterator = eventStore.query("tipo3", 10, 90);
        while(eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.println(event.timestamp());
        }

        System.out.println("--------------------------------");

        eventIterator = eventStore.query("tipo4", 10, 90);
        //eventIterator.current();
        while(eventIterator.moveNext()) {
            Event event = eventIterator.current();
            System.out.println(event.timestamp());
        }
    }
}
