package mvm.rya.api.utils;

/*
 * #%L
 * mvm.rya.rya.api
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

import info.aduna.iteration.CloseableIteration;

import java.util.Enumeration;

/**
 * Date: 7/26/12
 * Time: 9:12 AM
 */
public class EnumerationWrapper<E, X extends Exception> implements CloseableIteration<E, X> {
    private Enumeration<E> enumeration;

    public EnumerationWrapper(Enumeration<E> enumeration) {
        this.enumeration = enumeration;
    }

    @Override
    public void close() throws X {
        //nothing
    }

    @Override
    public boolean hasNext() throws X {
        return enumeration.hasMoreElements();
    }

    @Override
    public E next() throws X {
        return enumeration.nextElement();
    }

    @Override
    public void remove() throws X {
        enumeration.nextElement();
    }
}
