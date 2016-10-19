/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rya.jena.legacy.graph.query;

import org.apache.jena.graph.Node_Literal;

/**
 * Rewrite - class which does expression rewrites for Query
 */
public class Rewrite {
    public static Expression rewriteStringMatch(final Expression e) {
        final Expression L = e.getArg(0), R = e.getArg(1);
        final PatternLiteral pattern = Rewrite.getPattern(R);
        if (pattern == null) {
            return e;
        } else if (isStartsWith(pattern)) {
            return startsWith(L, pattern.getPatternString().substring(1), pattern.getPatternModifiers());
        } else if (isContains(pattern)) {
            return contains(L, pattern.getPatternString(), pattern.getPatternModifiers());
        } else if (isEndsWith(pattern)) {
            return endsWith(L, front(pattern.getPatternString()), pattern.getPatternModifiers());
        }
        return e;
    }

    protected static String front(final String s) {
        return s.substring(0, s.length() - 1);
    }

    public static PatternLiteral getPattern(final Expression E) {
        if (E instanceof PatternLiteral) {
            final PatternLiteral L = (PatternLiteral) E;
            if (L.getPatternLanguage().equals(PatternLiteral.RDQL)) {
                return L;
            }
        }
        return null;
    }

    /*
     * This following code for the evaluators is horrid - there's too much uncaptured
     * variation. Perhaps it will all go away when I understand how to use the
     * regex package (note, not the Java 1.4 package, because we still support
     * Java 1.3; the ORO package that RDQL uses).
     *
     * TODO clean this up.
     */

    public static abstract class DyadicLiteral extends Dyadic {
        public DyadicLiteral(final Expression L, final String F, final String R) {
            super(L, F, new Expression.Fixed(R));
        }

        @Override
        public boolean evalBool(final Object l, final Object r) {
            return evalBool(nodeAsString(l), r.toString());
        }

        protected String nodeAsString(final Object object) {
            return object instanceof Node_Literal ? ((Node_Literal) object).getLiteralLexicalForm() : object.toString();
        }

        protected abstract boolean evalBool(String l, String r);
    }

    public static Expression endsWith(final Expression L, final String content, final String modifiers) {
        if (modifiers.equals("i")) {
            return new DyadicLiteral(L, ExpressionFunctionURIs.J_endsWithInsensitive, content) {
                protected final String lowerContent = content.toLowerCase();

                @Override
                public boolean evalBool(final String l, final String r) {
                    return l.toLowerCase().endsWith(lowerContent);
                }
            };
        } else {
            return new DyadicLiteral(L, ExpressionFunctionURIs.J_EndsWith, content) {
                @Override
                public boolean evalBool(final String l, final String r) {
                    return l.endsWith(r);
                }
            };
        }
    }

    public static Expression startsWith(final Expression L, final String content, final String modifiers) {
        if (modifiers.equals("i")) {
            return new DyadicLiteral(L, ExpressionFunctionURIs.J_startsWithInsensitive, content) {
                protected final String lowerContent = content.toLowerCase();

                @Override
                public boolean evalBool(final String l, final String r) {
                    return l.toLowerCase().startsWith(lowerContent);
                }
            };
        } else {
            return new DyadicLiteral(L, ExpressionFunctionURIs.J_startsWith, content) {
                @Override
                public boolean evalBool(final String l, final String r) {
                    return l.startsWith(r);
                }
            };
        }
    }

    public static Expression contains(final Expression L, final String content, final String modifiers) {
        if (modifiers.equals("i")) {
            return new DyadicLiteral(L, ExpressionFunctionURIs.J_containsInsensitive, content) {
                protected final String lowerContent = content.toLowerCase();

                @Override
                public boolean evalBool(final String l, final String r) {
                    return l.toLowerCase().indexOf(lowerContent) > -1;
                }
            };
        } else {
            return new DyadicLiteral(L, ExpressionFunctionURIs.J_contains, content) {
                @Override
                public boolean evalBool(final String l, final String r) {
                    return l.indexOf(r) > -1;
                }
            };
        }
    }

    public static boolean notSpecial(final String pattern) {
        return pattern.matches("[A-Za-z0-9-_:/ ]*");
    }

    public static boolean isContains(final PatternLiteral pattern) {
        return notSpecial(pattern.getPatternString()) && iOnly(pattern.getPatternModifiers());
    }

    protected static boolean iOnly(final String modifiers) {
        return modifiers.equals("") || modifiers.equals("i");
    }

    public static boolean isStartsWith(final PatternLiteral pattern) {
        final String s = pattern.getPatternString();
        return s.startsWith("^") && notSpecial(s.substring(1));
    }

    public static boolean isEndsWith(final PatternLiteral pattern) {
        final String s = pattern.getPatternString();
        return s.endsWith("$") && notSpecial(s.substring(0, s.length() - 1));
    }
}