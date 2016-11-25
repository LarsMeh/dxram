package de.hhu.bsinfo.dxram.stats;

import java.util.Collection;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Exposing the component backend to the front with some
 * additional features like printing, filtering, ...
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class StatisticsService extends AbstractDXRAMService {

    /**
     * Constructor
     */
    public StatisticsService() {
        super("stats");
    }

    /**
     * Get all available statistic recorders
     *
     * @return Get recorders
     */
    public static Collection<StatisticsRecorder> getRecorders() {
        return StatisticsRecorderManager.getRecorders();
    }

    /**
     * Print the statistics of all created recorders to the console.
     */
    public static void printStatistics() {
        StatisticsRecorderManager.getRecorders().forEach(System.out::println);
    }

    /**
     * Print the statistics of a specific recorder to the console.
     *
     * @param p_className
     *     Fully qualified name of the class including package location (or relative to de.hhu.bsinfo)
     */
    public static void printStatistics(final String p_className) {
        Class<?> clss;
        try {
            clss = Class.forName(p_className);
        } catch (final ClassNotFoundException e) {
            // check again with longest common prefix of package names
            try {
                clss = Class.forName("de.hhu.bsinfo." + p_className);
            } catch (final ClassNotFoundException ignored) {
                return;
            }
        }

        printStatistics(clss);
    }

    /**
     * Reset all statistics
     */
    public static void resetStatistics() {
        for (StatisticsRecorder recorder : StatisticsRecorderManager.getRecorders()) {
            recorder.reset();
        }
    }

    /**
     * Print the statistics of a specific recorder to the console.
     *
     * @param p_class
     *     Class this recorder was created for.
     */
    private static void printStatistics(final Class<?> p_class) {
        StatisticsRecorder recorder = StatisticsRecorderManager.getRecorder(p_class);
        if (recorder != null) {
            System.out.println(recorder);
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        // no dependencies
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

}
