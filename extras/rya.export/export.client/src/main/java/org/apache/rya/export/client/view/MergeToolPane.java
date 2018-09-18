package org.apache.rya.export.client.view;

import static java.util.Objects.requireNonNull;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.Date;

import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.policy.TimestampPolicyStatementStore;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.MemoryMerger;
import org.apache.rya.export.client.merge.future.RepSynchRunnableFactory.StatementListener;
import org.apache.rya.export.client.view.LogPane.CancelListener;

/**
 * Displays the configuration and logging during a merge.
 */
public class MergeToolPane extends JPanel {
    private static final long serialVersionUID = 1L;

    private final ConfigPane configPane;
    private final LogPane logPane;

    private final RyaStatementStore sourceStore;
    private final RyaStatementStore destStore;
    private final String ryaInstanceName;
    private final long timeOffset;

    /**
     * Constructs a new {@link MergeToolPane}.
     *
     * @param sourceStore - The source store of the rep/synch. (not null)
     * @param destStore - The destination store of the rep/synch. (not null)
     * @param ryaInstanceName - The Rya instance to perfrom the rep/synch over. (not null)
     * @param timeOffset - The server time offset.
     */
    public MergeToolPane(final RyaStatementStore sourceStore, final RyaStatementStore destStore, final String ryaInstanceName,
            final long timeOffset) {
        this.sourceStore = requireNonNull(sourceStore);
        this.destStore = requireNonNull(destStore);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        this.timeOffset = timeOffset;

        configPane = new ConfigPane();
        logPane = new LogPane();

        //when next is clicked, move the divider.
        configPane.addRunMergeListener(() -> {
            configPane.setVisible(false);
            logPane.setVisible(true);

            runMerger(configPane.getDateTime());
        });

        initComponents();
    }

    private void runMerger(final Date dateTime) {
        final TimestampPolicyStatementStore sourceTimestampStore = new TimestampPolicyStatementStore(sourceStore, dateTime.getTime());
        final MemoryMerger merger = new MemoryMerger(sourceTimestampStore, destStore, ryaInstanceName, timeOffset);


        final CancelListener cancelListen = () -> merger.cancel();
        logPane.addCancelListener(cancelListen);
        //find the number of statements to process.
        long max = sourceTimestampStore.count();
        if(max != -1) {
            final long destCount = destStore.count();
            if(destCount != -1) {
                max += destCount;
            }
        }

        logPane.setMaxProgress((int)max);

        final StatementListener listen = new StatementListener() {
            @Override
            public void notifyAddStatement(final RyaStatementStore store, final RyaStatement statement) {
                SwingUtilities.invokeLater(() -> logPane.addLog(String.format("Adding: %s", statement.toString())));
            }

            @Override
            public void notifyRemoveStatement(final RyaStatementStore store, final RyaStatement statement) {
                SwingUtilities.invokeLater(() -> logPane.addLog(String.format("Removing: %s", statement.toString())));
            }

            @Override
            public void updateCount() {
                logPane.updateProgress();
            }
        };
        merger.addStatementListener(listen);

        final SwingWorker<Void, Void> repSynchWorker = new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                merger.runJob();
                logPane.removeCancelListener(cancelListen);
                return null;
            }
        };
        repSynchWorker.execute();
    }

    private void initComponents() {
        setLayout(new GridBagLayout());
        final GridBagConstraints g = new GridBagConstraints();
        g.gridx = 0;
        g.gridy = 0;
        g.weightx = 1.0;
        g.weighty = 1.0;
        g.gridheight = 1;
        g.gridwidth = 1;
        g.fill = GridBagConstraints.BOTH;
        add(configPane, g);

        add(logPane, g);
        logPane.setVisible(false);
    }
}
