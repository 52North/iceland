/*
 * Copyright 2015-2017 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.n52.iceland.config;

import java.util.Set;
import java.util.function.Function;

import org.n52.iceland.config.json.JsonActivationDao;
import org.n52.iceland.ogc.sos.SosOfferingExtensionKey;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * TODO JavaDoc
 *
 * @author Christian Autermann
 */
public class SosJsonActivationDao extends JsonActivationDao implements SosActivationDao {

    private static final String OFFERING_EXTENSIONS = "offeringExtensions";

    @Override
    public boolean isOfferingExtensionActive(SosOfferingExtensionKey key) {
        return isActive(OFFERING_EXTENSIONS, matches(key), true);
    }

    @Override
    public void setOfferingExtensionStatus(SosOfferingExtensionKey key, boolean active) {
        setStatus(OFFERING_EXTENSIONS, matches(key), s -> encode(s, key), active);
    }

    @Override
    public Set<SosOfferingExtensionKey> getOfferingExtensionKeys() {
        Function<JsonNode, SosOfferingExtensionKey> fun = createDomainDecoder(SosOfferingExtensionKey::new);
        return getKeys(OFFERING_EXTENSIONS, fun);
    }

}
