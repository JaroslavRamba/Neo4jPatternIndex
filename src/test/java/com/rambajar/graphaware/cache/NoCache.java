/*
 * Copyright (c) 2014 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.rambajar.graphaware.cache;

import com.graphaware.test.performance.CacheConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link CacheConfiguration} representing no caches available => reading from disk.
 */
public class NoCache implements CacheConfiguration {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean needsWarmup() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> addToConfig(Map<String, String> existingConfig) {
        Map<String, String> result = new HashMap<>(existingConfig);

        //low level cache
        result.put("dbms.pagecache.memory", "10k"); //can't set this to 0, bug in Neo4j

        //high level cache
        result.put("cache_type", "none");

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "nocache";
    }
}