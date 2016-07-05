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
package org.apache.rya.indexing.pcj.fluo.client.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;

/**
 * A human readable report that displays the title of a {@link ReportItem} on the
 * left hand side of the table and the value of the item on the right hand side.
 * If an item does not have any values, then it prints an empty line.
 */
@Immutable
@ParametersAreNonnullByDefault
public class Report {

    private final ImmutableList<ReportItem> items;
    private final int maxTitleLength;
    private final int maxValueLineLength;

    /**
     * Use an instance of {@link Report.Builder} to construct instances of this class.
     *
     * @param items - An ordered list of items that appear in the report. (not null)
     * @param maxTitleLength - The length of the longest title in the report. (> 0)
     * @param maxValueLineLength - The length of the longest value line in the report. (> 0)
     */
    private Report(
            final ImmutableList<ReportItem> items,
            final int maxTitleLength,
            final int maxValueLineLength) {
        this.items = checkNotNull(items);
        checkArgument(maxTitleLength > 0);
        this.maxTitleLength = maxTitleLength;
        checkArgument(maxValueLineLength > 0);
        this.maxValueLineLength = maxValueLineLength;
    }

    @Override
    public String toString() {
        // Figure out how long each line will be.
        final int lineLength = "| ".length() + maxTitleLength + " | ".length() + maxValueLineLength + " |".length();

        // Format that may be used to write each line.
        final String lineFormat = "| %-" + maxTitleLength + "s | %-" + maxValueLineLength +  "s |\n";

        // The line that is used as the first and last line of the report.
        final String dashLine = StringUtils.repeat("-", lineLength);

        // Build the String verison of the report.
        final StringBuilder builder = new StringBuilder();
        builder.append(dashLine).append("\n");

        for(final ReportItem item : items) {
            final String[] valueLines = item.getValueLines();
            switch(valueLines.length) {
                case 0:
                    // Write an empty value cell.
                    builder.append( String.format(lineFormat, item.getTitle(), "") );
                    break;

                case 1:
                    // Write the value cell.
                    builder.append( String.format(lineFormat, item.getTitle(), valueLines[0]) );
                    break;

                default:
                    builder.append( String.format(lineFormat, item.getTitle(), valueLines[0]) );
                    for(int i = 1; i < valueLines.length; i++) {
                        builder.append( String.format(lineFormat, "", valueLines[i]) );
                    }
                    break;
            }
        }

        builder.append(dashLine).append("\n");
        return builder.toString();
    }

    /**
     * @return Creates a new {@link Report.Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * An item that may appear within a {@link Report}. Each item has a title
     * that briefly describes the value it holds.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static final class ReportItem {
        private final String title;
        private final String[] valueLines;

        /**
         * Constructs an instance of {@link ReportItem} when there is no value
         * associated with the title.
         *
         * @param title - Describes the item's value. (not null)
         */
        public ReportItem(final String title) {
            this.title = checkNotNull(title);
            this.valueLines = new String[0];
        }

        /**
         * Constructs an instance of {@link ReportItem} when the value section
         * is contained within a single line.
         *
         * @param title - Describes the item's value. (not null)
         * @param valueLine - The line that will appear within the value section. (not null)
         */
        public ReportItem(final String title, final String valueLine) {
            this.title = checkNotNull(title);
            checkNotNull(valueLine);
            this.valueLines = new String[]{ valueLine };
        }

        /**
         * Constructs an instance of {@link ReportItem} when the value section
         * spans many lines.
         *
         * @param title - Describes the item's value. (not null)
         * @param valueLines - The value section broken into lines as they
         *   will appear within the report. (not null)
         */
        public ReportItem(final String title, final String[] valueLines) {
            this.title = checkNotNull(title);
            this.valueLines = checkNotNull(valueLines);
        }

        /**
         * @return Describes the item's value.
         */
        public String getTitle() {
            return title;
        }

        /**
         * @return The value section broken into lines as they will appear within the report.
         */
        public String[] getValueLines() {
            return valueLines;
        }
    }

    /**
     * Builds instances of {@link Report}.
     */
    @ParametersAreNonnullByDefault
    public static final class Builder {

        private final ImmutableList.Builder<ReportItem> lines = ImmutableList.builder();
        private int maxTitleLength = 0;
        private int maxValueLineLength = 0;

        /**
         * Append a {@link ReportItem} to the end of the {@link Report}.
         *
         * @param item - The next item that will appear in the report. (not null)
         * @return This builder so that method invocations may be chained.
         */
        public Builder appendItem(final ReportItem item) {
            checkNotNull(item);
            lines.add(item);

            final int titleLength = item.getTitle().length();
            if(maxTitleLength < titleLength) {
                this.maxTitleLength = titleLength;
            }

            for(final String valueLine : item.getValueLines()) {
                final int valueLineLength = valueLine.length();
                if(maxValueLineLength < valueLineLength) {
                    this.maxValueLineLength = valueLineLength;
                }
            }

            return this;
        }

        /**
         * @return An instance of {@link Report} using the state of the builder.
         */
        public Report build() {
            return new Report( lines.build(), maxTitleLength, maxValueLineLength);
        }
    }
}