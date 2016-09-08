/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.export.client.conf;

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
import javax.swing.WindowConstants;

import com.toedter.calendar.JCalendar;

/**
 * Dialog for picking date and time.
 */
public class DateTimePickerDialog extends JDialog {
    private static final long serialVersionUID = 1L;

    private JCalendar dateChooser;
    private JSpinner timeSpinner;

    private Date selectedDateTime;
    private final JLabel label;


    /**
     * Creates a new instance of {@link DateTimePickerDialog}.
     * @param title the title to display up top.
     * @param message the message to display.
     */
    public DateTimePickerDialog(final String title, final String message) {
        this(null, title, message);
    }

    /**
     * Creates a new instance of {@link DateTimePickerDialog}.
     * @param date the initial date to have the dialog show.
     * @param title the title to display up top.
     * @param message the message to display.
     */
    public DateTimePickerDialog(final Date date, final String title, final String message) {
        // Create a modal dialog
        super((JDialog) null);
        setTitle(title);
        setModalityType(ModalityType.APPLICATION_MODAL);
        setType(Type.NORMAL);

        setLayout(new GridBagLayout());
        setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        final JButton okButton = new JButton("OK");
        okButton.addActionListener (new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent event) {
                selectedDateTime = findSelectedDateTime();

                // Hide dialog
                setVisible(false);
            }
        });

        getRootPane().setDefaultButton(okButton);

        final JPanel dateTimePanel = buildDateTimePanel(date);
        label = new JLabel (message);
        label.setBorder(BorderFactory.createEtchedBorder());

        final GridBagConstraints c = new GridBagConstraints();
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

    private JPanel buildDateTimePanel(final Date date) {
        final JPanel datePanel = new JPanel();

        dateChooser = new JCalendar();
        if (date != null) {
            final Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            dateChooser.setCalendar(calendar);
        }

        datePanel.add(dateChooser);

        final SpinnerModel model = new SpinnerDateModel();
        timeSpinner = new JSpinner(model);
        final DateEditor editor = new DateEditor(timeSpinner, "HH:mm:ss");
        timeSpinner.setEditor(editor);
        if (date != null) {
            timeSpinner.setValue(date);
        }

        datePanel.add(timeSpinner);

        return datePanel;
    }

    private Date findSelectedDateTime() {
        // Get the values from the date chooser
        final int day = dateChooser.getDayChooser().getDay();
        final int month = dateChooser.getMonthChooser().getMonth();
        final int year = dateChooser.getYearChooser().getYear();

        // Get the values from the time chooser
        final Calendar timeCalendar = Calendar.getInstance();
        timeCalendar.setTime((Date) timeSpinner.getValue());
        final int hour = timeCalendar.get(Calendar.HOUR_OF_DAY);
        final int minute = timeCalendar.get(Calendar.MINUTE);
        final int second = timeCalendar.get(Calendar.SECOND);

        // Combine these values into a single date object
        final Calendar newCalendar = Calendar.getInstance();
        newCalendar.set(Calendar.YEAR, year);
        newCalendar.set(Calendar.MONTH, month);
        newCalendar.set(Calendar.DATE, day);
        newCalendar.set(Calendar.HOUR_OF_DAY, hour);
        newCalendar.set(Calendar.MINUTE, minute);
        newCalendar.set(Calendar.SECOND, second);

        final Date newDate = newCalendar.getTime();

        return newDate;
    }

    /**
     * @return the selected date time.
     */
    public Date getSelectedDateTime() {
        return selectedDateTime;
    }
}
