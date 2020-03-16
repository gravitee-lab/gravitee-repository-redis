/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.repository.redis.management.internal.impl;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.management.api.search.AlertEventCriteria;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.redis.management.internal.AlertEventRedisRepository;
import io.gravitee.repository.redis.management.model.RedisAlertEvent;
import io.gravitee.repository.redis.management.model.RedisEvent;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@Component
public class AlertEventRedisRepositoryImpl extends AbstractRedisRepository implements AlertEventRedisRepository {

    private final static String REDIS_KEY = "alert_events";

    @Override
    public Page<RedisAlertEvent> search(AlertEventCriteria criteria, Pageable pageable) {
        Set<String> filterKeys = new HashSet<>();
        String tempDestination = "tmp-" + Math.abs(criteria.hashCode());

        /*
        // Implement OR clause for event type
        if (! filter.getTypes().isEmpty()) {
            filter.getTypes().forEach(type -> filterKeys.add(REDIS_KEY + ":type:" + type));
            redisTemplate.opsForSet().unionAndStore(null, filterKeys, tempDestination);
            filterKeys.clear();
            filterKeys.add(tempDestination);
        }

        // Add clause based on event properties
        Set<String> internalUnionFilter = new HashSet<>();
        filter.getProperties().forEach((propertyKey, propertyValue) -> {
            if (propertyValue instanceof Collection) {
                Set<String> collectionFilter = new HashSet<>(((Collection) propertyValue).size());
                String collectionTempDestination = "tmp-" + propertyKey + ":" + propertyValue.hashCode();
                ((Collection) propertyValue).forEach(value ->
                        collectionFilter.add(REDIS_KEY + ":" + propertyKey + ":" + value));
                redisTemplate.opsForZSet().unionAndStore(null, collectionFilter, collectionTempDestination);
                internalUnionFilter.add(collectionTempDestination);
                filterKeys.add(collectionTempDestination);
            } else {

            }
        });
         */

        if (criteria.getAlert() != null && !criteria.getAlert().isEmpty()) {
            filterKeys.add(criteria.getAlert());
        }

        // And finally add clause based on event creation date
        filterKeys.add(REDIS_KEY + ":created_at");

        redisTemplate.opsForZSet().intersectAndStore(null, filterKeys, tempDestination);

        Set<Object> keys;
        long total;

        if (criteria.getFrom() != 0 && criteria.getTo() != 0) {
            keys = redisTemplate.opsForZSet().rangeByScore(tempDestination,
                    criteria.getFrom(), criteria.getTo(), 0, Long.MAX_VALUE);
            total = keys.size();
            if (pageable != null) {
                keys = redisTemplate.opsForZSet().reverseRangeByScore(
                        tempDestination,
                        criteria.getFrom(), criteria.getTo(),
                        pageable.from(), pageable.pageSize());
            } else {
                keys = redisTemplate.opsForZSet().reverseRangeByScore(
                        tempDestination,
                        criteria.getFrom(), criteria.getTo());
            }
        } else {
            keys = redisTemplate.opsForZSet().rangeByScore(tempDestination,
                    0, Long.MAX_VALUE);
            total = keys.size();
            if (pageable != null) {
                keys = redisTemplate.opsForZSet().reverseRangeByScore(
                        tempDestination,
                        0, Long.MAX_VALUE,
                        pageable.from(), pageable.pageSize());
            } else {
                keys = redisTemplate.opsForZSet().reverseRangeByScore(
                        tempDestination,
                        0, Long.MAX_VALUE);
            }
        }

        redisTemplate.opsForZSet().removeRange(tempDestination, 0, -1);
    //    internalUnionFilter.forEach(dest -> redisTemplate.opsForZSet().removeRange(dest, 0, -1));
        List<Object> eventObjects = redisTemplate.opsForHash().multiGet(REDIS_KEY, keys);

        return new Page<>(
                eventObjects.stream()
                        .map(event -> convert(event, RedisAlertEvent.class))
                        .collect(Collectors.toList()),
                (pageable != null) ? pageable.pageNumber() : 0,
                keys.size(),
                total);
    }

    @Override
    public RedisAlertEvent findById(final String event) {
        Object alert = redisTemplate.opsForHash().get(REDIS_KEY, event);
        if (alert == null) {
            return null;
        }

        return convert(alert, RedisAlertEvent.class);
    }

    @Override
    public RedisAlertEvent saveOrUpdate(final RedisAlertEvent event) {
        redisTemplate.executePipelined((RedisConnection connection) ->  {
            redisTemplate.opsForHash().put(REDIS_KEY, event.getId(), event);
            redisTemplate.opsForSet().add(event.getAlert(), event.getId());
            redisTemplate.opsForZSet().add(REDIS_KEY + ":created_at", event.getId(), event.getCreatedAt());
            return null;
        });
        return event;
    }

    @Override
    public void delete(final String event) {
        final RedisAlertEvent redisEvent = findById(event);

        redisTemplate.executePipelined((RedisConnection connection) ->  {
            redisTemplate.opsForHash().delete(REDIS_KEY, event);
            redisTemplate.opsForSet().remove(redisEvent.getAlert(), redisEvent.getId());
            redisTemplate.opsForZSet().remove(REDIS_KEY + ":created_at", redisEvent.getId());
            return null;
        });
    }
}
