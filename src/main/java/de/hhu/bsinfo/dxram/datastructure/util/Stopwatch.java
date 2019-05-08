package de.hhu.bsinfo.dxram.datastructure.util;

import java.util.LinkedList;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, Mar 9, 2019
 *
 */
public final class Stopwatch {

    public static final Stopwatch GLOBAL = new Stopwatch();

    private long m_start = 0;
    private long m_stop = 0;
    private final LinkedList<StopwatchEntry> m_splits = new LinkedList<>();

    public void start() {
        m_start = System.nanoTime();
    }

    public void split() {
        m_splits.add(new StopwatchEntry(System.nanoTime(), null));
    }

    public void split(String comment) {
        m_splits.add(new StopwatchEntry(System.nanoTime(), comment));
    }

    public void stop() {
        m_stop = System.nanoTime();
        m_splits.addFirst(new StopwatchEntry(m_start, null));
        m_splits.addLast(new StopwatchEntry(m_stop, "STOP"));
    }

    public void reset() {
        m_splits.clear();
    }

    public String history() {
        long total = (m_stop - m_start);
        String unit = "ns";
        if (total > 10000000) {
            unit = "ms";
            total /= 1000000;
        } else if (total > 10000) {
            unit = "µs";
            total /= 1000;
        }
        StringBuilder stmp = new StringBuilder(String.format("Stopwatch [%d %s]:\n", total, unit));
        for (int i = 1; i < m_splits.size(); i++) {
            if (m_splits.get(i).getComment() == null) {
                continue;
            }
            long delta = m_splits.get(i).getTime() - m_splits.get(i-1).getTime();
            if (delta > 10000000) {
                unit = "ms";
                delta /= 1000000;
            } else if (delta > 10000) {
                unit = "µs";
                delta /= 1000;
            } else {
                unit = "ns";
            }
            stmp.append(String.format(">>> [%s] - %d %s\n", m_splits.get(i).getComment(), delta, unit));
        }
        return stmp.toString();
    }

    public final class StopwatchEntry {
        private final long m_time;
        private final String m_comment;

        public StopwatchEntry(final long time, final String comment) {
            m_time = time;
            m_comment = comment;
        }

        public long getTime() {
            return m_time;
        }

        public String getComment() {
            return m_comment;
        }
    }
}
