package org.apache.rya.export.client.view;

import static java.util.Objects.requireNonNull;
import static javax.swing.ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED;
import static javax.swing.ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.EventListener;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

/**
 * View for logging the Rya Merge Tool as it runs.
 */
public class LogPane extends JPanel {
    private static final long serialVersionUID = 1L;

    private final JProgressBar progressBar;
    private final JLabel statementLabel;
    private final JScrollPane scroll;
    private final JTextArea logArea;
    private final JButton showLogButton;
    private final JButton cancelButton;
    private final JButton doneButton;

    /**
     * Constructs a new {@link LogPane}.
     */
    public LogPane() {
        progressBar = new JProgressBar();
        logArea = new JTextArea();
        scroll = new JScrollPane(logArea);
        statementLabel = new JLabel("Replicated statements: ");
        cancelButton = new JButton("Cancel");
        doneButton = new JButton("Done");
        cancelButton.setToolTipText("Cancels the current running replication/synchronization");

        cancelButton.addActionListener(e -> {
            cancelButton.setEnabled(false);
            doneButton.setEnabled(true);

            final CancelListener[] listeners = listenerList.getListeners(CancelListener.class);
            for(final CancelListener listen : listeners) {
                listen.notifyCancel();
            }
        });

        showLogButton = new JButton("Show Statements");
        showLogButton.addActionListener(e -> {
            scroll.setVisible(!scroll.isVisible());
            statementLabel.setVisible(scroll.isVisible());

            final ResizeListener[] listeners = listenerList.getListeners(ResizeListener.class);
            for(final ResizeListener listen : listeners) {
                listen.notifyResize(scroll.isVisible());
            }

            showLogButton.setText(logArea.isVisible() ? "Hide Statements" : "Show Statements");
        });

        progressBar.addChangeListener(e -> {
            if(progressBar.getValue() == progressBar.getMaximum()) {
                doneButton.setEnabled(true);
            }
        });

        doneButton.addActionListener(e -> {
            System.exit(1);
        });

        initComponents();
    }

    /**
     * @param maxProgress - The total number of statements to process.
     */
    public void setMaxProgress(final int maxProgress) {
        if(maxProgress == -1) {
            progressBar.setIndeterminate(true);
        } else {
            progressBar.setMaximum(maxProgress);
        }
    }

    public void updateProgress() {
        if(!progressBar.isIndeterminate()) {
            progressBar.setValue(progressBar.getValue() + 1);
        }
    }

    /**
     * @param log - The log to display in the log area. (not null)
     */
    public void addLog(final String log) {
        requireNonNull(log);
        logArea.append(log + "\n");

        updateProgress();
    }

    /**
     * @param listen
     */
    public void addCancelListener(final CancelListener listen) {
        requireNonNull(listen);
        listenerList.add(CancelListener.class, listen);
    }

    /**
     * @param listen - Removes the provided CancelListener
     * from this pane to no longer be notified to cancel the merge. (not null)
     */
    public void removeCancelListener(final CancelListener listen) {
        requireNonNull(listen);
        listenerList.remove(CancelListener.class, listen);
    }

    /**
     * @param listen - The {@link ResizeListener} to add. (not null)
     */
    public void addResizeListener(final ResizeListener listen) {
        requireNonNull(listen);
        listenerList.add(ResizeListener.class, listen);
    }

    /**
     * @param listen - Removes the provided ResizeListener. (not null)
     */
    public void removeResizeListener(final ResizeListener listen) {
        requireNonNull(listen);
        listenerList.remove(ResizeListener.class, listen);
    }

    /**
     * Initialize the starting view state.
     */
    private void initComponents() {
        setLayout(new GridBagLayout());
        final GridBagConstraints g = new GridBagConstraints();
        g.gridx = 0;
        g.gridy = 0;
        g.weightx = 1.0;
        g.weighty = 0;
        g.gridheight = 1;

        g.fill = GridBagConstraints.HORIZONTAL;
        g.gridwidth = GridBagConstraints.REMAINDER;
        g.insets = new Insets(25, 25, 0, 25);
        add(progressBar, g);

        g.gridy++;
        g.gridwidth = 1;
        g.fill = GridBagConstraints.NONE;
        g.anchor = GridBagConstraints.WEST;
        add(statementLabel, g);
        statementLabel.setVisible(false);

        g.gridy++;
        g.weighty = 1.0;
        g.gridwidth = GridBagConstraints.REMAINDER;
        g.fill = GridBagConstraints.BOTH;
        g.insets = new Insets(0, 25, 0, 25);
        add(scroll, g);
        scroll.setHorizontalScrollBarPolicy(HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scroll.setVerticalScrollBarPolicy(VERTICAL_SCROLLBAR_AS_NEEDED);
        scroll.setVisible(false);
        logArea.setEditable(false);

        g.gridy++;
        g.weighty = 0;
        g.weightx = 0;
        g.gridwidth = 1;
        g.fill = GridBagConstraints.NONE;
        g.anchor = GridBagConstraints.WEST;
        add(cancelButton, g);

        g.gridx++;
        g.anchor = GridBagConstraints.EAST;
        add(showLogButton, g);

        g.gridy++;
        g.insets = new Insets(0, 0, 25, 25);
        add(doneButton, g);
        doneButton.setEnabled(false);
    }

    /**
     * Listener to used to notify when the window should resize.
     */
    public interface ResizeListener extends EventListener {
        /**
         * Notification that the window should be resized.
         * @param logShown - Whether or not the statement log is shown.
         */
        public void notifyResize(final boolean logShown);
    }

    /**
     * Listener to be used to notify when a merge/synch/clone should be canceled.
     */
    interface CancelListener extends EventListener {
        /**
         * Notifies the listener that the current running merge should be canceled.
         */
        public void notifyCancel();
    }
}
