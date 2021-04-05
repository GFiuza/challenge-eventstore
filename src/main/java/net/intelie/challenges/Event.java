package net.intelie.challenges;

/**
 * This is just an event stub, feel free to expand it if needed.
 */
public class Event implements Comparable<Event>{
    /*
     * The elements in the ConcurrentSkipListSet needs to implements the Comparable interface. Because every insertion in this data structure is already ordered.
     * And i've chosen to compare Events first by type, and then by timestamp. That's because of the removeAll operation.
     * So it's easier to find a event by it's type only, and the removeAll will be faster that way.
     */
    private final String type;
    private final long timestamp;

    public Event(String type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public String type() {
        return type;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(Event o) {
        Integer eventTypeComparison = this.type().compareTo(o.type());
        if (eventTypeComparison.equals(0)) {
            if(this.timestamp() < o.timestamp()) {
                return -1;
            } else if (this.timestamp() > o.timestamp()) {
                return 1;
            }
            return 0;
        }else {
            return eventTypeComparison;
        }
    }
}
