package org.apache.rya.export.client.view;

import static java.util.Calendar.HOUR;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.SECOND;
import static java.util.Objects.requireNonNull;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Calendar;
import java.util.Date;
import java.util.EventListener;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.JSpinner.DateEditor;
import javax.swing.JTextField;
import javax.swing.SpinnerDateModel;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.DateFormatter;

import com.toedter.calendar.JDateChooser;

/**
 * A panel to pick a date/time combination to use when
 * replicating/synchronizing a rya instance.
 */
public class ConfigPane extends JPanel {
    private static final long serialVersionUID = 1L;

    private final JDateChooser calendarField;
    private final JSpinner timeField;
    private final SpinnerDateModel dateModel;
    private final DateEditor timeEditor;

    private final JButton nextButton;

    /**
     * Constructs a new {@link ConfigPane}.
     */
    public ConfigPane() {
        calendarField = new JDateChooser();

        dateModel = new SpinnerDateModel();
        dateModel.setCalendarField(Calendar.SECOND);
        timeField = new JSpinner(dateModel);
        timeEditor = new DateEditor(timeField, "HH:mm:ss");

        final DateFormatter formatter = (DateFormatter) timeEditor.getTextField().getFormatter();
        formatter.setAllowsInvalid(false);
        formatter.setOverwriteMode(true);

        nextButton = new JButton("Run");
        nextButton.setEnabled(false);
        nextButton.setToolTipText("Run the replication/synchronization");
        nextButton.addActionListener(e -> {
            final RunMergeListener[] listeners = listenerList.getListeners(RunMergeListener.class);
            for(final RunMergeListener listen : listeners) {
                listen.notifyRun();
            }
        });

        //nextbutton enabled state
        final JTextField cF = (JTextField) calendarField.getDateEditor().getUiComponent();
        cF.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void removeUpdate(final DocumentEvent e) {}
            @Override
            public void changedUpdate(final DocumentEvent e) { }
            @Override
            public void insertUpdate(final DocumentEvent e) {
                //since the time config field can't be checked, only the calendar field will be used to check state
                nextButton.setEnabled(calendarField.getCalendar() != null);
            }
        });

        initComponents();
    }

    /**
     * @return - The configured Date/Time to use when replicating/synchronizing.
     */
    public Date getDateTime() {
        final Calendar date = calendarField.getCalendar();
        final Date chosenTime = dateModel.getDate();
        date.set(HOUR, chosenTime.getHours());
        date.set(MINUTE, chosenTime.getMinutes());
        date.set(SECOND, chosenTime.getSeconds());
        return date.getTime();
    }

    /**
     * @param listen - Adds the provided {@link RunMergeListener}
     * to be notified when the rep/synch should be run. (not null)
     */
    public void addRunMergeListener(final RunMergeListener listen) {
        requireNonNull(listen);
        listenerList.add(RunMergeListener.class, listen);
    }

    /**
     * @param listen - removes the provided {@link RunMergeListener}
     * from this {@link ConfigPane}. (not null)
     */
    public void remove(final RunMergeListener listen) {
        requireNonNull(listen);
        listenerList.remove(RunMergeListener.class, listen);
    }

    /**
     * Initialize the starting view state.
     */
    private void initComponents() {
        setLayout(new GridBagLayout());
        final GridBagConstraints g = new GridBagConstraints();
        g.gridx = 0;
        g.gridy = 0;
        g.weightx = 0;
        g.weighty = 1.0;
        g.gridheight = 1;
        g.gridwidth = 1;
        g.insets = new Insets(0, 25, 0, 0);
        final JLabel cloneTimeLabel = new JLabel("Clone Date/Time: ");
        add(cloneTimeLabel, g);

        g.gridx++;
        g.weightx = 1.0;
        g.fill = GridBagConstraints.HORIZONTAL;
        g.insets = new Insets(0, 0, 0, 25);
        add(calendarField, g);

        g.gridx++;
        add(timeEditor, g);

        g.gridy++;
        g.weightx = 1.0;
        g.fill = GridBagConstraints.NONE;
        g.insets = new Insets(0, 0, 0, 0);
        add(nextButton, g);
    }

    /**
     * Listener to be notified when the merge tool should be run against
     * the configured fields in the {@link ConfigPane}.
     */
    interface RunMergeListener extends EventListener {
        /**
         * Notifies the listener that the merge tool should run with the current configuration.
         */
        public void notifyRun();
    }
}
