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

import io.gravitee.repository.exceptions.TechnicalException;
import io.gravitee.repository.management.api.TokenRepository;
import io.gravitee.repository.management.model.Token;
import io.gravitee.repository.redis.management.internal.TokenRedisRepository;
import io.gravitee.repository.redis.management.model.RedisToken;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author GraviteeSource Team
 */
@Component
public class RedisTokenRepository implements TokenRepository {

    @Autowired
    private TokenRedisRepository tokenRedisRepository;

    @Override
    public Optional<Token> findById(final String tokenId) throws TechnicalException {
        final RedisToken redisToken = tokenRedisRepository.findById(tokenId);
        return Optional.ofNullable(convert(redisToken));
    }

    @Override
    public Token create(final Token token) throws TechnicalException {
        final RedisToken redisToken = tokenRedisRepository.saveOrUpdate(convert(token));
        return convert(redisToken);
    }

    @Override
    public Token update(final Token token) throws TechnicalException {
        if (token == null || token.getName() == null) {
            throw new IllegalStateException("Token to update must have a name");
        }

        final RedisToken redisToken = tokenRedisRepository.findById(token.getId());

        if (redisToken == null) {
            throw new IllegalStateException(String.format("No token found with name [%s]", token.getId()));
        }

        final RedisToken redisTokenUpdated = tokenRedisRepository.saveOrUpdate(convert(token));
        return convert(redisTokenUpdated);
    }

    @Override
    public Set<Token> findAll() throws TechnicalException {
        final Set<RedisToken> tokens = tokenRedisRepository.findAll();

        return tokens.stream()
                .map(this::convert)
                .collect(Collectors.toSet());
    }

    @Override
    public void delete(final String tokenId) throws TechnicalException {
        tokenRedisRepository.delete(tokenId);
    }

    @Override
    public List<Token> findByReference(String referenceType, String referenceId) throws TechnicalException {
        final List<RedisToken> token = tokenRedisRepository.findByReferenceTypeAndReferenceId(referenceType, referenceId);
        return token.stream()
                .map(this::convert)
                .collect(Collectors.toList());
    }

    private Token convert(final RedisToken redisToken) {
        final Token token = new Token();
        token.setId(redisToken.getId());
        token.setName(redisToken.getName());
        token.setToken(redisToken.getToken());
        token.setReferenceId(redisToken.getReferenceId());
        token.setReferenceType(redisToken.getReferenceType());
        token.setCreatedAt(redisToken.getCreatedAt());
        token.setLastUseAt(redisToken.getLastUseAt());
        token.setExpiresAt(redisToken.getExpiresAt());
        return token;
    }

    private RedisToken convert(final Token token) {
        final RedisToken redisToken = new RedisToken();
        redisToken.setId(token.getId());
        redisToken.setName(token.getName());
        redisToken.setToken(token.getToken());
        redisToken.setReferenceId(token.getReferenceId());
        redisToken.setReferenceType(token.getReferenceType());
        redisToken.setCreatedAt(token.getCreatedAt());
        redisToken.setLastUseAt(token.getLastUseAt());
        redisToken.setExpiresAt(token.getExpiresAt());
        return redisToken;
    }
}
