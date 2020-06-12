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

import io.gravitee.repository.redis.management.internal.TokenRedisRepository;
import io.gravitee.repository.redis.management.model.RedisToken;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author GraviteeSource Team
 */
@Component
public class TokenRedisRepositoryImpl extends AbstractRedisRepository implements TokenRedisRepository {

    private final static String REDIS_KEY = "token";

    @Override
    public Set<RedisToken> findAll() {
        final Map<Object, Object> tokens = redisTemplate.opsForHash().entries(REDIS_KEY);
        return tokens.values()
                .stream()
                .map(object -> convert(object, RedisToken.class))
                .collect(Collectors.toSet());
    }

    @Override
    public RedisToken findById(final String tokenId) {
        Object token = redisTemplate.opsForHash().get(REDIS_KEY, tokenId);
        return convert(token, RedisToken.class);
    }

    @Override
    public RedisToken saveOrUpdate(final RedisToken token) {
        redisTemplate.executePipelined((RedisConnection connection) ->  {
            redisTemplate.opsForHash().put(REDIS_KEY, token.getId(), token);
            redisTemplate.opsForSet().add(getTokenKey(token.getReferenceType(), token.getReferenceId()), token.getId());
            return null;
        });
        return token;
    }

    @Override
    public void delete(final String token) {
        redisTemplate.opsForHash().delete(REDIS_KEY, token);
    }

    @Override
    public List<RedisToken> findByReferenceTypeAndReferenceId(String referenceType, String referenceId) {
        final Set<Object> keys = redisTemplate.opsForSet().members(getTokenKey(referenceType, referenceId));
        final List<Object> values = redisTemplate.opsForHash().multiGet(REDIS_KEY, keys);
        return values.stream()
                .filter(Objects::nonNull)
                .map(token -> convert(token, RedisToken.class))
                .collect(toList());
    }

    private String getTokenKey(final String referenceType, final String referenceId) {
        return referenceType + ":" + referenceId;
    }
}
