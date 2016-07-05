package mvm.rya.accumulo.mr.merge.gui;

/*
 * #%L
 * mvm.rya.accumulo.mr.merge
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Calendar;
import java.util.Date;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.JSpinner.DateEditor;
import javax.swing.SpinnerDateModel;
import javax.swing.SpinnerModel;

import com.toedter.calendar.JCalendar;

/**
 * Dialog for picking date and time.
 */
public class DateTimePickerDialog extends JDialog {
    private static final long serialVersionUID = 1L;

    private JCalendar dateChooser;
    private JSpinner timeSpinner;

    private Date selectedDateTime;
    private JLabel label;


    /**
     * Creates a new instance of {@link DateTimePickerDialog}.
     * @param title the title to display up top.
     * @param message the message to display.
     */
    public DateTimePickerDialog(String title, String message) {
        this(null, title, message);
    }

    /**
     * Creates a new instance of {@link DateTimePickerDialog}.
     * @param date the initial date to have the dialog show.
     * @param title the title to display up top.
     * @param message the message to display.
     */
    public DateTimePickerDialog(Date date, String title, String message) {
        // Create a modal dialog
        super((JDialog) null);
        setTitle(title);
        setModalityType(ModalityType.APPLICATION_MODAL);
        setType(Type.NORMAL);

        setLayout(new GridBagLayout());
        setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);

        JButton okButton = new JButton("OK");
        okButton.addActionListener (new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent event) {
                selectedDateTime = findSelectedDateTime();

                // Hide dialog
                setVisible(false);
            }
        });

        getRootPane().setDefaultButton(okButton);

        JPanel dateTimePanel = buildDateTimePanel(date);
        label = new JLabel (message);
        label.setBorder(BorderFactory.createEtchedBorder());

        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(5, 5, 5, 5);
        c.gridx = 0;
        c.gridy = 0;

        add(dateTimePanel, c);
        c.gridy++;
        add(label, c);
        c.anchor = GridBagConstraints.EAST;
        c.fill = GridBagConstraints.NONE;
        c.gridy++;
        add(okButton, c);

        pack();
    }

    private JPanel buildDateTimePanel(Date date) {
        JPanel datePanel = new JPanel();

        dateChooser = new JCalendar();
        if (date != null) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            dateChooser.setCalendar(calendar);
        }

        datePanel.add(dateChooser);

        SpinnerModel model = new SpinnerDateModel();
        timeSpinner = new JSpinner(model);
        DateEditor editor = new DateEditor(timeSpinner, "HH:mm:ss");
        timeSpinner.setEditor(editor);
        if (date != null) {
            timeSpinner.setValue(date);
        }

        datePanel.add(timeSpinner);

        return datePanel;
    }

    private Date findSelectedDateTime() {
        // Get the values from the date chooser
        int day = dateChooser.getDayChooser().getDay();
        int month = dateChooser.getMonthChooser().getMonth();
        int year = dateChooser.getYearChooser().getYear();

        // Get the values from the time chooser
        Calendar timeCalendar = Calendar.getInstance();
        timeCalendar.setTime((Date) timeSpinner.getValue());
        int hour = timeCalendar.get(Calendar.HOUR_OF_DAY);
        int minute = timeCalendar.get(Calendar.MINUTE);
        int second = timeCalendar.get(Calendar.SECOND);

        // Combine these values into a single date object
        Calendar newCalendar = Calendar.getInstance();
        newCalendar.set(Calendar.YEAR, year);
        newCalendar.set(Calendar.MONTH, month);
        newCalendar.set(Calendar.DATE, day);
        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, minute);
        newCalendar.set(Calendar.SECOND, second);

        Date newDate = newCalendar.getTime();

        return newDate;
    }

    /**
     * @return the selected date time.
     */
    public Date getSelectedDateTime() {
        return selectedDateTime;
    }
}
