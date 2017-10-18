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
package org.apache.rya.api.path;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility methods for {@link Path}s.
 */
public final class PathUtils {
    /**
     * Private constructor to prevent instantiation.
     */
    private PathUtils() {
    }

    /**
     * Cleans the path to prevent path manipulation. It performs the following:
     * <ul>
     * <li>Normalizes a path, removing double and single dot path steps.</li>
     * </ul>
     * @param path the path to clean.
     * @return the cleansed path.
     * @throws IllegalArgumentException if file is in a shared directory.
     */
    public static Path cleanPath(final Path path) {
        if (path != null) {
            final Path cleanPath = cleanPath(path.toString());
            return cleanPath;
        }
        return null;
    }

    /**
     * Cleans the path to prevent path manipulation. It performs the following:
     * <ul>
     * <li>Normalizes a path, removing double and single dot path steps.</li>
     * </ul>
     * @param filename the filename to clean.
     * @return the cleansed path.
     * @throws IllegalArgumentException if file is in a shared directory.
     */
    public static Path cleanPath(final String filename) {
        if (filename != null) {
            final Path cleanPath = Paths.get(clean(filename));
            return cleanPath;
        }
        return null;
    }

    /**
     * Cleans the path to prevent path manipulation. It performs the following:
     * <ul>
     *   <li>Normalizes a path, removing double and single dot path steps.</li>
     *   <li>Ensures path is not in shared directory.</li>
     * </ul>
     * @param filename the filename to clean.
     * @return the cleansed path.
     * @throws IllegalArgumentException if file is in a shared directory.
     * @throws IOException
     */
    public static org.apache.hadoop.fs.Path cleanHadoopPath(final org.apache.hadoop.fs.Path hadoopPath, final Configuration conf) throws IllegalArgumentException, IOException {
        if (hadoopPath != null) {
            final Path path = fromHadoopPath(hadoopPath, conf);
            final Path clean = cleanPath(path);
            return toHadoopPath(clean);
        }
        return null;
    }

    /**
     * Cleans the path to prevent path manipulation. It performs the following:
     * <ul>
     *   <li>Normalizes a path, removing double and single dot path steps.</li>
     *   <li>Ensures path is not in shared directory.</li>
     * </ul>
     * @param filename the filename to clean.
     * @return the cleansed path.
     * @throws IllegalArgumentException if file is in a shared directory.
     */
    public static String clean(final String filename) throws IllegalArgumentException {
        if (filename != null) {
            final String clean = FilenameUtils.normalize(filename);
            if (!isInSecureDir(clean)) {
                throw new IllegalArgumentException("Operation of a file in a shared directory is not allowed: " + filename);
            }
            return clean;
        }
        return null;
    }

    /**
     * Indicates whether file lives in a secure directory relative to the
     * program's user.
     * @param filename the filename to test.
     * @return {@code true} if file's directory is secure.
     */
    public static boolean isInSecureDir(final String filename) {
        final Path path = filename != null ? Paths.get(filename) : null;
        return isInSecureDir(path, null);
    }

    /**
     * Indicates whether file lives in a secure directory relative to the
     * program's user.
     * @param file {@link Path} to test.
     * @return {@code true} if file's directory is secure.
     */
    public static boolean isInSecureDir(final Path file) {
        return isInSecureDir(file, null);
    }

    /**
     * Indicates whether file lives in a secure directory relative to the
     * program's user.
     * @param file {@link Path} to test.
     * @param user {@link UserPrincipal} to test. If {@code null}, defaults to
     * current user
     * @return {@code true} if file's directory is secure.
     */
    public static boolean isInSecureDir(final Path file, final UserPrincipal user) {
        return isInSecureDir(file, user, 5);
    }

    /**
     * Indicates whether file lives in a secure directory relative to the
     * program's user.
     * @param file {@link Path} to test.
     * @param user {@link UserPrincipal} to test. If {@code null}, defaults to
     * current user.
     * @param symlinkDepth Number of symbolic links allowed.
     * @return {@code true} if file's directory is secure.
     */
    public static boolean isInSecureDir(Path file, UserPrincipal user, final int symlinkDepth) {
        if (!file.isAbsolute()) {
            file = file.toAbsolutePath();
        }
        if (symlinkDepth <= 0) {
            // Too many levels of symbolic links
            return false;
        }
        // Get UserPrincipal for specified user and superuser
        final Path fileRoot = file.getRoot();
        if (fileRoot == null) {
            return false;
        }
        final FileSystem fileSystem = Paths.get(fileRoot.toString()).getFileSystem();
        final UserPrincipalLookupService upls = fileSystem.getUserPrincipalLookupService();
        UserPrincipal root = null;
        try {
            if (SystemUtils.IS_OS_UNIX) {
                root = upls.lookupPrincipalByName("root");
            } else {
                root = upls.lookupPrincipalByName("Administrators");
            }
            if (user == null) {
                user = upls.lookupPrincipalByName(System.getProperty("user.name"));
            }
            if (root == null || user == null) {
                return false;
            }
        } catch (final IOException x) {
            return false;
        }
        // If any parent dirs (from root on down) are not secure, dir is not secure
        for (int i = 1; i <= file.getNameCount(); i++) {
            final Path partialPath = Paths.get(fileRoot.toString(), file.subpath(0, i).toString());
            try {
                if (Files.isSymbolicLink(partialPath)) {
                    if (!isInSecureDir(Files.readSymbolicLink(partialPath), user, symlinkDepth - 1)) {
                        // Symbolic link, linked-to dir not secure
                        return false;
                    }
                } else {
                    final UserPrincipal owner = Files.getOwner(partialPath);
                    if (!user.equals(owner) && !root.equals(owner)) {
                        // dir owned by someone else, not secure
                        return SystemUtils.IS_OS_UNIX ? false : Files.isWritable(partialPath);
                    }
                }
            } catch (final IOException x) {
                return false;
            }
        }
        return true;
    }

    /**
     * Converts a path string to a {@link org.apache.hadoop.fs.Path}.
     * @param filename The path string
     * @return the resulting {@link org.apache.hadoop.fs.Path}.
     */
    public static org.apache.hadoop.fs.Path toHadoopPath(final String filename) {
        if (filename != null) {
            final Path path = Paths.get(filename);
            return toHadoopPath(path);
        }
        return null;
    }

    /**
     * Converts a {@link Path} to a {@link org.apache.hadoop.fs.Path}.
     * @param path The {@link Path}.
     * @return the resulting {@link org.apache.hadoop.fs.Path}.
     */
    public static org.apache.hadoop.fs.Path toHadoopPath(final Path path) {
        if (path != null) {
            final String stringPath = FilenameUtils.separatorsToUnix(path.toAbsolutePath().toString());
            final org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(stringPath);
            return hadoopPath;
        }
        return null;
    }

    /**
     * Converts a {@link org.apache.hadoop.fs.Path} to a {@link Path}.
     * @param hadoopPath The {@link org.apache.hadoop.fs.Path}.
     * @param conf the {@link Configuration}.
     * @return the resulting {@link org.apache.hadoop.fs.Path}.
     */
    public static Path fromHadoopPath(final org.apache.hadoop.fs.Path hadoopPath, final Configuration conf) throws IOException {
        if (hadoopPath != null) {
            final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(hadoopPath.toUri(), conf);
            final File tempFile = File.createTempFile(hadoopPath.getName(), "");
            tempFile.deleteOnExit();
            fs.copyToLocalFile(hadoopPath, new org.apache.hadoop.fs.Path(tempFile.getAbsolutePath()));
            return tempFile.toPath();
        }
        return null;
    }


}