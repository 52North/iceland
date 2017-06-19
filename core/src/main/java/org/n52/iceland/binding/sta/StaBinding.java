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
package org.n52.iceland.binding.sta;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.n52.faroe.annotation.Configurable;
import org.n52.faroe.annotation.Setting;
import org.n52.iceland.binding.BindingConstants;
import org.n52.iceland.binding.BindingKey;
import org.n52.iceland.binding.MediaTypeBindingKey;
import org.n52.iceland.binding.PathBindingKey;
import org.n52.iceland.binding.SimpleBinding;
import org.n52.iceland.coding.decode.OwsDecodingException;
import org.n52.iceland.exception.HTTPException;
import org.n52.iceland.service.ServiceSettings;
import org.n52.janmayen.Json;
import org.n52.janmayen.http.MediaType;
import org.n52.janmayen.http.MediaTypes;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.ows.service.OwsServiceRequest;
import org.n52.shetland.ogc.ows.service.OwsServiceResponse;
import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.shetland.ogc.sos.SosConstants;
import org.n52.shetland.ogc.sta.StaSettings;
import org.n52.svalbard.decode.Decoder;
import org.n52.svalbard.decode.OperationDecoderKey;
import org.n52.svalbard.decode.exception.DecodingException;
import org.n52.svalbard.decode.exception.NoDecoderForKeyException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Binding} implementation for JSON encoded RESTful requests as used in SensorThings API
 *
 * @author <a href="mailto:m.kiesow@52north.org">Martin Kiesow</a>
 */
@Configurable
public class StaBinding extends SimpleBinding {
    private static final String CONFORMANCE_CLASS
            = "http://www.opengis.net/spec/SOS/2.0/conf/json";
    private static final Set<String> CONFORMANCE_CLASSES = Collections
            .singleton(CONFORMANCE_CLASS);
    private static final Logger LOG = LoggerFactory.getLogger(StaBinding.class);
    private static final String SERVICE = "service";
    private static final String VERSION = "version";
    private static final String REQUEST = "request";

    private String serviceURL;

    private static final ImmutableSet<BindingKey> KEYS = ImmutableSet.<BindingKey>builder()
            .add(new PathBindingKey(BindingConstants.STA_BINDING_ENDPOINT))
            .add(new MediaTypeBindingKey(MediaTypes.APPLICATION_STA))
            .build();

    @Override
    protected boolean isUseHttpResponseCodes() {
        return true;
    }

    @Override
    protected MediaType getDefaultContentType() {
        // TODO change to APPLICATION_STA?
        //return MediaTypes.APPLICATION_JSON;
        return MediaTypes.APPLICATION_STA;
    }

    @Override
    public String getUrlPattern() {
        return BindingConstants.STA_BINDING_ENDPOINT;
    }

    @Override
    public Set<String> getConformanceClasses(String service, String version) {
        if(SosConstants.SOS.equals(service) && Sos2Constants.SERVICEVERSION.equals(version)) {
            return Collections.unmodifiableSet(CONFORMANCE_CLASSES);
        }
        return Collections.emptySet();
    }

    @Override
    public Set<BindingKey> getKeys() {
        return Collections.unmodifiableSet(KEYS);
    }

    public String getServiceURL() {
        return this.serviceURL;
    }

    @Setting(ServiceSettings.SERVICE_URL)
    public void setServiceURL(final URI url) {
        this.serviceURL = url.toString();
    }

    @Override
    public void doPostOperation(HttpServletRequest req, HttpServletResponse res) throws HTTPException,
            IOException {

        StaSettings.getInstance().setServiceURL(serviceURL);
        StaSettings.getInstance().setBindingEndpoint(BindingConstants.STA_BINDING_ENDPOINT);

        OwsServiceRequest request = null;
        try {
            request = parseRequest(req);
            checkServiceOperatorKeyTypes(request);

            //TODO make STA a supported service and create an OwsServiceKey from service and version)
            OwsServiceResponse response = getServiceOperator(request).receiveRequest(request);
            writeResponse(req, res, response);

        } catch (OwsExceptionReport oer) {
            oer.setVersion(request != null ? request.getVersion() : null);
            LOG.warn("Unexpected error", oer);
            writeOwsExceptionReport(req, res, oer);
        }
    }

    private OwsServiceRequest parseRequest(HttpServletRequest request)
            throws OwsExceptionReport {
        try {
            JsonNode json = Json.loadReader(request.getReader());
            if (LOG.isDebugEnabled()) {
                LOG.debug("JSON-REQUEST: {}", Json.print(json));
            }
            OperationDecoderKey key = new OperationDecoderKey(
                    json.path(SERVICE).textValue(),
                    json.path(VERSION).textValue(),
                    json.path(REQUEST).textValue(),
                    MediaTypes.APPLICATION_JSON);
            Decoder<OwsServiceRequest, JsonNode> decoder =
                    getDecoder(key);

            if (decoder == null) {
                NoDecoderForKeyException cause = new NoDecoderForKeyException(key);
                throw new NoApplicableCodeException().withMessage(cause.getMessage()).causedBy(cause);
            }

            OwsServiceRequest sosRequest;
            try {
                sosRequest = decoder.decode(json);
            } catch (OwsDecodingException ex) {
                throw ex.getCause();
            } catch (DecodingException ex) {
                throw new NoApplicableCodeException().withMessage(ex.getMessage()).causedBy(ex);
            }

            sosRequest.setRequestContext(getRequestContext(request));
            return sosRequest;

        } catch (IOException ioe) {
            throw new NoApplicableCodeException().causedBy(ioe).withMessage(
                    "Error while reading request! Message: %s", ioe.getMessage());
        }
    }
}
