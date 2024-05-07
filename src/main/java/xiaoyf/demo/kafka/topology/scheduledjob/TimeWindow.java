package xiaoyf.demo.kafka.topology.scheduledjob;

public record TimeWindow(long start, long end) implements Comparable<TimeWindow> {

    public static TimeWindow windowFor(long timestamp, long windowSize) {

        long remainMillis = timestamp % windowSize;

        if (remainMillis == 0) {
            return new TimeWindow(
                    timestamp,
                    timestamp + windowSize);
        }

        return new TimeWindow(
                timestamp - remainMillis,
                timestamp - remainMillis + windowSize);
    }

    @Override
    public String toString() {
        return "Window{" +
                "start=" + DateUtils.format(start) +
                ", end=" + DateUtils.format(end) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeWindow window = (TimeWindow) o;
        return start == window.start && end == window.end;
    }

    @Override
    public int compareTo(TimeWindow o) {
        long delta = this.start - o.start;
        if (delta > 0) return 1;
        if (delta < 0) return -1;

        delta = this.end - o.end;
        if (delta > 0) return 1;
        if (delta < 0) return -1;

        return 0;
    }

    public boolean isClosable(long triggeringTimestamp, long grace) {
        return end + grace <= triggeringTimestamp;
    }

}
