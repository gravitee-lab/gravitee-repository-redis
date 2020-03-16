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
package io.gravitee.repository.redis.management;

import io.gravitee.common.data.domain.Page;
import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.AlertEventRepository;
import io.gravitee.repository.management.api.search.AlertEventCriteria;
import io.gravitee.repository.management.api.search.Pageable;
import io.gravitee.repository.management.api.search.builder.PageableBuilder;
import io.gravitee.repository.management.model.AlertEvent;
import io.gravitee.repository.redis.management.internal.AlertEventRedisRepository;
import io.gravitee.repository.redis.management.model.RedisAlertEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
@Component
public class RedisAlertEventRepository implements AlertEventRepository {

    @Autowired
    private AlertEventRedisRepository alertEventRedisRepository;

    @Override
    public Optional<AlertEvent> findById(final String event) throws TechnicalException {
        final RedisAlertEvent redisAlertEvent = alertEventRedisRepository.findById(event);
        return Optional.ofNullable(convert(redisAlertEvent));
    }

    @Override
    public Page<AlertEvent> search(AlertEventCriteria criteria, Pageable pageable) {
        Page<RedisAlertEvent> eventsPage = alertEventRedisRepository.search(criteria, pageable);

        return new Page<>(
                eventsPage.getContent().stream().map(this::convert).collect(Collectors.toList()),
                eventsPage.getPageNumber(), (int) eventsPage.getPageElements(),
                eventsPage.getTotalElements());
    }

    @Override
    public AlertEvent create(final AlertEvent event) throws TechnicalException {
        final RedisAlertEvent redisAlertEvent = alertEventRedisRepository.saveOrUpdate(convert(event));
        return convert(redisAlertEvent);
    }

    @Override
    public AlertEvent update(final AlertEvent event) throws TechnicalException {
        if (event == null || event.getId() == null) {
            throw new IllegalStateException("Alert event to update must have an ID");
        }

        final RedisAlertEvent redisAlertEvent = alertEventRedisRepository.findById(event.getId());

        if (redisAlertEvent == null) {
            throw new IllegalStateException(String.format("No alert event found with id [%s]", event.getId()));
        }

        final RedisAlertEvent redisAlertEventUpdated = alertEventRedisRepository.saveOrUpdate(convert(event));
        return convert(redisAlertEventUpdated);
    }

    @Override
    public void delete(final String alertId) throws TechnicalException {
        alertEventRedisRepository.delete(alertId);
    }

    @Override
    public void deleteAll(String alertId) {
        AlertEventCriteria criteria = new AlertEventCriteria.Builder().alert(alertId).build();
        Pageable pageable = new PageableBuilder().pageNumber(0).pageSize(Integer.MAX_VALUE).build();
        for (RedisAlertEvent redisAlertEvent : alertEventRedisRepository.search(criteria, pageable).getContent()) {
            alertEventRedisRepository.delete(redisAlertEvent.getId());
        }

    }

    private AlertEvent convert(final RedisAlertEvent redisAlertEvent) {
        final AlertEvent event = new AlertEvent();
        event.setId(redisAlertEvent.getId());
        event.setAlert(redisAlertEvent.getAlert());
        event.setMessage(redisAlertEvent.getMessage());
        event.setCreatedAt(new Date(redisAlertEvent.getCreatedAt()));
        event.setUpdatedAt(new Date(redisAlertEvent.getUpdatedAt()));
        return event;
    }

    private RedisAlertEvent convert(final AlertEvent event) {
        final RedisAlertEvent redisAlertEvent = new RedisAlertEvent();
        redisAlertEvent.setId(event.getId());
        redisAlertEvent.setAlert(event.getAlert());
        redisAlertEvent.setMessage(event.getMessage());
        redisAlertEvent.setCreatedAt(event.getCreatedAt().getTime());
        if (event.getUpdatedAt() != null) {
            redisAlertEvent.setUpdatedAt(event.getUpdatedAt().getTime());
        }
        return redisAlertEvent;
    }
}
