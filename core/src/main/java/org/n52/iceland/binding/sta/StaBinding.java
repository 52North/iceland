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
import com.google.common.base.Enums;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.n52.faroe.annotation.Configurable;
import org.n52.faroe.annotation.Setting;
import org.n52.iceland.binding.BindingKey;
import org.n52.iceland.binding.MediaTypeBindingKey;
import org.n52.iceland.binding.SimpleBinding;
import org.n52.iceland.exception.HTTPException;
import org.n52.iceland.service.ServiceSettings;
import org.n52.janmayen.http.HTTPMethods;
import org.n52.janmayen.http.MediaType;
import org.n52.janmayen.http.MediaTypes;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.ows.service.OwsServiceRequest;
import org.n52.shetland.ogc.ows.service.OwsServiceResponse;
import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.shetland.ogc.sos.SosConstants;
import org.n52.shetland.ogc.sta.StaConstants;
import org.n52.shetland.ogc.sta.StaConstants.PathSegment;
import org.n52.shetland.ogc.sta.StaSettings;
import org.n52.shetland.ogc.sta.request.StaGetDatastreamsRequest;
import org.n52.shetland.ogc.sta.request.StaGetRequest;
import org.n52.svalbard.decode.AbstractStaRequestDecoder;
import org.n52.svalbard.decode.Decoder;
import org.n52.svalbard.decode.OperationDecoderKey;
import org.n52.svalbard.decode.exception.DecodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.n52.iceland.binding.Binding} implementation for JSON encoded RESTful requests as used in SensorThings API
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
            .add(new MediaTypeBindingKey(MediaTypes.APPLICATION_STA))
            .build();

    @Override
    protected boolean isUseHttpResponseCodes() {
        return true;
    }

    @Override
    protected MediaType getDefaultContentType() {
        return MediaTypes.APPLICATION_STA;
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
    public void doGetOperation(HttpServletRequest request, HttpServletResponse response) throws HTTPException, IOException {

        StaSettings.getInstance().setServiceURL(serviceURL);

        OwsServiceRequest owsRequest = null;
        try {

            owsRequest = parseRequest(request);
            checkServiceOperatorKeyTypes(owsRequest);

            //TODO make STA a supported service and create an OwsServiceKey from service and version)
            OwsServiceResponse owsResponse = getServiceOperator(owsRequest).receiveRequest(owsRequest);
            writeResponse(request, response, owsResponse);

        } catch (OwsExceptionReport oer) {
            oer.setVersion(request != null ? owsRequest.getVersion() : null);
            LOG.warn("Unexpected error", oer);
            writeOwsExceptionReport(request, response, oer);
        }
    }

    @Override
    public void doPostOperation(HttpServletRequest req, HttpServletResponse res) throws HTTPException,
            IOException {

        StaSettings.getInstance().setServiceURL(serviceURL);

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

            String accept = request.getHeader("Accept");
            String contentType = request.getHeader("Content-Type");

            // serviceURI "http.../52n-sos-webapp/service"
            String requestURI = request.getRequestURI(); // "/52n-sos-webapp/sta/v1.0/Datastreams", "/52n-sos-webapp/service"
            String requestQuery = request.getQueryString(); // "$count=true&$top=5&$skip=2", "service=STA&version=v1.0&request=GetObservation"

            String decodablePath = extractDecodablePath(serviceURL, requestURI);

            // get service version
            String serviceVersion;
            String resourcePath = decodablePath;
            if (decodablePath.startsWith("/" + StaConstants.VERSION_1_0)) {
                serviceVersion = StaConstants.VERSION_1_0;
                resourcePath = resourcePath.replace("/" + StaConstants.VERSION_1_0, "");

            } else {
                DecodingException cause = new DecodingException("Service version not supported.", request);
                throw new NoApplicableCodeException().withMessage(cause.getMessage()).causedBy(cause);
            }

            // get path parameters as list
            List<PathSegment> pathList = new ArrayList<>();
            if (resourcePath.isEmpty() || resourcePath.equals("/")) {
                // TODO return links to entity sets (at least for GET)

            } else {
                decodeResourcePath(resourcePath, pathList);
            }

            // get query parameters as map
            LinkedHashMap<StaConstants.QueryOption, String> queryOptions = null;
            if (requestQuery != null && !requestQuery.trim().equals("")) {
                decodeQueryOptions(requestQuery.trim());
            }

            // get resource type (to determine which request type to choose)
            // TODO remove segment and id, only resourceType is needed here
            StaConstants.PathSegment resourceSegment;
            StaConstants.EntityPathComponent resourceType;
            String resourceId;
            // TODO replace with loop
            if (pathList.size() > 0 && pathList.get(pathList.size() - 1).getComponent() instanceof StaConstants.EntityPathComponent) {
                resourceSegment = pathList.get(pathList.size() - 1);
            } else if (pathList.size() > 1 && pathList.get(pathList.size() - 2).getComponent() instanceof StaConstants.EntityPathComponent) {
                resourceSegment = pathList.get(pathList.size() - 2);
            } else if (pathList.size() > 2 && pathList.get(pathList.size() - 3).getComponent() instanceof StaConstants.EntityPathComponent) {
                resourceSegment = pathList.get(pathList.size() - 3);
            } else {
                throw new IOException("There is no detectable resource in path '" + pathList.toString() + "'");
            }

            if (resourceSegment != null) {
                resourceType = (StaConstants.EntityPathComponent) resourceSegment.getComponent();
                resourceId = resourceSegment.getId();
            } else {
                throw new IOException("There is no detectable resource in path '" + pathList.toString() + "'");
            }

            // create request
            OwsServiceRequest sosRequest = null;
            switch (request.getMethod()) {
                case HTTPMethods.GET:
                    if (StaConstants.EntitySet.Datastreams == resourceType) {

//                        sosRequest = new StaGetDatastreamsRequest(StaConstants.SERVICE_NAME, serviceVersion);
//
//                        ((StaGetRequest) sosRequest).setPath(pathList);
//                        ((StaGetRequest) sosRequest).setQueryOptions(queryOptions);
//
//                        sosRequest.setRequestContext(getRequestContext(request));

                    } else if (StaConstants.EntitySet.FeaturesOfInterest == resourceType) {

                    } else if (StaConstants.EntitySet.HistoricalLocations == resourceType) {

                    } else if (StaConstants.EntitySet.Locations == resourceType) {

                    } else if (StaConstants.EntitySet.Observations == resourceType) {

                        Decoder<OwsServiceRequest, JsonNode> decoder;
                        if (resourceSegment.getId() == null || resourceSegment.getId().equals("")) {

                            // get decoder
                            decoder = getDecoder(new OperationDecoderKey(StaConstants.SERVICE_NAME,
                                    serviceVersion, StaConstants.Operation.GET_OBSERVATIONS, MediaTypes.APPLICATION_STA));

                        } else {
                            // get decoder
                            decoder = getDecoder(new OperationDecoderKey(StaConstants.SERVICE_NAME,
                                    serviceVersion, StaConstants.Operation.GET_OBSERVATIONS_WITH_ID, MediaTypes.APPLICATION_STA));
                        }
                        // set resource path and query options
                        setStaParameters(decoder, resourceSegment, pathList, queryOptions);

//                        sosRequest.setRequestContext(getRequestContext(request));

                        // decode request
                        try {
                            sosRequest = decoder.decode(null);

                        } catch (DecodingException de) {
                            throw new IOException("GET Observations request could not be decoded: " + de.getMessage());
                        }


                    } else if (StaConstants.EntitySet.ObservedProperties == resourceType) {

                    } else if (StaConstants.EntitySet.Sensors == resourceType) {

                    } else if (StaConstants.EntitySet.Things == resourceType) {

                    } else if (StaConstants.Entity.Datastream == resourceType) {
                        sosRequest = new StaGetDatastreamsRequest(StaConstants.SERVICE_NAME, serviceVersion);

                        ((StaGetRequest) sosRequest).setPath(pathList);
                        ((StaGetRequest) sosRequest).setQueryOptions(queryOptions);

                        sosRequest.setRequestContext(getRequestContext(request));

                    } else if (StaConstants.Entity.FeatureOfInterest == resourceType) {

                    } else if (StaConstants.Entity.HistoricalLocation == resourceType) {

                    } else if (StaConstants.Entity.Location == resourceType) {

                    } else if (StaConstants.Entity.Observation == resourceType) {

                    } else if (StaConstants.Entity.ObservedProperty == resourceType) {

                    } else if (StaConstants.Entity.Sensor == resourceType) {

                    } else if (StaConstants.Entity.Thing == resourceType) {

                    } else {
                        throw new IOException("STA EntitySet '" + pathList.get(0) + "' is not supported.");
                    }
                    break;

                case HTTPMethods.POST:
                    throw new IOException("HTTP POST not supported yet.");

                    // get resource path (there should be no query paramerters)
                    // get data

//                    try {
//                        JsonNode json = Json.loadReader(request.getReader());
//                        if (LOG.isDebugEnabled()) {
//                            LOG.debug("JSON-REQUEST: {}", Json.print(json));
//                        }
//                        OperationDecoderKey key = new OperationDecoderKey(
//                                json.path(SERVICE).textValue(),
//                                json.path(VERSION).textValue(),
//                                json.path(REQUEST).textValue(),
//                                MediaTypes.APPLICATION_JSON);
//                        Decoder<OwsServiceRequest, JsonNode> decoder = getDecoder(key);
//                        if (decoder == null) {
//                            NoDecoderForKeyException cause = new NoDecoderForKeyException(key);
//                            throw new NoApplicableCodeException().withMessage(cause.getMessage()).causedBy(cause);
//                        }
//
//                        try {
//                            ((StaPostRequest) sosRequest) = decoder.decode(json);
//                        } catch (OwsDecodingException ex) {
//                            throw ex.getCause();
//                        } catch (DecodingException ex) {
//                            throw new NoApplicableCodeException().withMessage(ex.getMessage()).causedBy(ex);
//                        }
//                        sosRequest.setRequestContext(getRequestContext(request));
//                        return sosRequest;
//                    } catch (IOException ioe) {
//                        throw new NoApplicableCodeException().causedBy(ioe).withMessage(
//                                "Error while reading request! Message: %s", ioe.getMessage());
//                    }

                case HTTPMethods.PATCH:
                    throw new IOException("HTTP PATCH not supported yet.");

                case HTTPMethods.DELETE:
                    throw new IOException("HTTP DELETE not supported yet.");

                default:
                    throw new IOException("Unsupported HTTP method.");
            }

            return sosRequest;

        } catch (IOException ioe) {
            throw new NoApplicableCodeException().causedBy(ioe).withMessage(
                    "Error while reading request! Message: %s", ioe.getMessage());
        }
    }

    /**
     * remove parts of the service url from the request path
     * @param serviceURL e.g. "http://localhost:8080/52n-sos-webapp/service"
     * @param requestURI e.g. "/52n-sos-webapp/service/sta/v1.0/Datastreams"
     * @return decodable path for STA, e.g. "/v1.0/Datastreams"
     */
    private String extractDecodablePath(String serviceURL, String requestURI) throws MalformedURLException {

        URL url = new URL(serviceURL);
        String service = serviceURL.replace(url.getProtocol() + "://", "");

        String urlAuthority = url.getAuthority();
        if (urlAuthority != null && !urlAuthority.isEmpty()) {

            service = service.replace(urlAuthority, "");
        }

        return requestURI.replace(service + StaConstants.STA_BINDING_ENDPOINT, "");
    }

    /**
     * decode a resource path
     * @param path resource path, e.g. "/Datastreams(1)/Observations"
     * @param list a list of Entities, Parameters or Options with optional IDs to receive the path components
     */
    private void decodeResourcePath(String path, List<StaConstants.PathSegment> list) throws CodedException {

        if (path == null || path.isEmpty()) {
            // abort

        } else if (path.indexOf("/") == 0) {
            // cut preceeding slash
            decodeResourcePath(path.substring(1), list);

        } else {
            // one or more path components left

            if (path.startsWith("$")) {
                // Option (always last)

                String option = (path.split("/"))[0];
                list.add(new PathSegment(StaConstants.Option.valueOf(option)));

                // there shall be no other components after this

            } else if (path.matches("^[a-zA-Z]+$")) {
                // last Entity, EntitySet or Parameter

                if (Enums.getIfPresent(StaConstants.Entity.class, path).orNull() != null) {
                    list.add(new PathSegment(StaConstants.Entity.valueOf(path)));

                } else if (Enums.getIfPresent(StaConstants.EntitySet.class, path).orNull() != null) {
                    list.add(new PathSegment(StaConstants.EntitySet.valueOf(path)));

                } else if (Enums.getIfPresent(StaConstants.Parameter.class, path).orNull() != null) {
                    list.add(new PathSegment(StaConstants.Parameter.valueOf(path)));

                } else {
                    throw new NoApplicableCodeException().withMessage("Error while reading request! Resource %s unavailable.", path);
                }

            } else if (path.matches("^[a-zA-Z]+/.*$")) {
                // Entity, EntitySet or Parameter

                String first = (path.split("/"))[0];

                if (Enums.getIfPresent(StaConstants.Entity.class, first).orNull() != null) {
                    list.add(new PathSegment(StaConstants.Entity.valueOf(first)));

                } else if (Enums.getIfPresent(StaConstants.EntitySet.class, first).orNull() != null) {
                    list.add(new PathSegment(StaConstants.EntitySet.valueOf(first)));

                } else if (Enums.getIfPresent(StaConstants.Parameter.class, first).orNull() != null) {
                    list.add(new PathSegment(StaConstants.Parameter.valueOf(first)));

                } else {
                    throw new NoApplicableCodeException().withMessage("Error while reading request! Resource %s unavailable.", first);
                }
                decodeResourcePath(path.replace(first, ""), list);

            } else if (path.matches("^[a-zA-Z]+\\(.+\\)$")) {
                // last EntitySet with ID

                String[] esid = path.split("\\(");

                if (esid.length != 2) {
                    throw new NoApplicableCodeException().withMessage("Error while reading request! Wrong resource path or identifier format.", "");

                } else if (Enums.getIfPresent(StaConstants.EntitySet.class, esid[0]).orNull() != null) {

                    // cut end brace
                    String id = esid[1].substring(0, esid[1].length() -1);
                    list.add(new PathSegment(StaConstants.EntitySet.valueOf(esid[0]), id));

                } else {
                    throw new NoApplicableCodeException().withMessage("Error while reading request! Resource %s is not an STA EntitySet.", path);
                }

            } else if (path.matches("^[a-zA-Z]+\\(.+\\)/.*$")) {
                // EntitySet with ID

                String first = (path.split("\\)/"))[0];
                String[] esid = first.split("\\(");

                if (esid.length != 2) {
                    throw new NoApplicableCodeException().withMessage("Error while reading request! Wrong resource path or identifier format.", "");

                } else if (Enums.getIfPresent(StaConstants.EntitySet.class, first).orNull() != null) {

                    // cut end brace
                    String id = esid[1].substring(0, esid[1].length() -1);
                    list.add(new PathSegment(StaConstants.EntitySet.valueOf(esid[0]), id));

                } else {
                    throw new NoApplicableCodeException().withMessage("Error while reading request! Resource %s is not an STA EntitySet.", first);
                }
                decodeResourcePath(path.replace(first + "\\)", ""), list);

            } else {

                throw new NoApplicableCodeException().withMessage("Error while reading request! Wrong resource path format.", "");
            }
        }
    }

    /**
     * decode a query string
     * @param path query string, e.g. "$top=10&$skip=5&$filter&$count=true"
     * @param map an ordered map of parameter names and values to receive the query options
     */
    private LinkedHashMap<StaConstants.QueryOption, String> decodeQueryOptions(String options) throws CodedException {

        if (options == null || options.isEmpty()) {
            return new LinkedHashMap<>(0);

        } else {

            String[] queryComponents = options.split("&");
            LinkedHashMap<StaConstants.QueryOption, String> map = new LinkedHashMap<>(queryComponents.length);

            for (String component : queryComponents) {
                String[] keyValue = component.split("=");
                String value = keyValue[1];

                if (value.isEmpty()) {
                    // TODO throw exception or log empty option

                } else if (Enums.getIfPresent(StaConstants.QueryOption.class, keyValue[0]).isPresent()) {

                    StaConstants.QueryOption key = StaConstants.QueryOption.valueOf(keyValue[0]);

                    // check for correct values
                    switch (key) {
                        case $count:
                            if (value.equals("true") || value.equals("false")) {
                                map.put(key, value);
                            } else {
                                // TODO throw exception
                            }
                            break;
                        case $skip:
                            map.put(key, String.valueOf(Integer.parseInt(value)));
                            break;
                        case $top:
                            map.put(key, String.valueOf(Integer.parseInt(value)));
                            break;
                        case $expand:
                            if (value.matches("^[a-zA-Z]+(/[a-zA-Z]+)*$")) {

                                map.put(key, value);
                            } else {
                                // TODO throw exception
                            }
                            break;
                        case $filter:
                            // TODO check
                            break;
                        case $orderby:
                            // TODO check
                            break;
                        case $select:
                            // TODO check
                            break;
                        default:
                            break;
                    }

                } else {
                    // TODO throw exception or log wrong option
                }
            }
            return map;
        }
    }

    private void setStaParameters(Decoder decoder, StaConstants.PathSegment resource, List<StaConstants.PathSegment> pathList, Map<StaConstants.QueryOption, String> queryOptions) throws IOException {

        if (decoder instanceof AbstractStaRequestDecoder) {

            AbstractStaRequestDecoder staDecoder = (AbstractStaRequestDecoder) decoder;

            staDecoder.setResource(resource);
            staDecoder.setPath(pathList);
            staDecoder.setQueryOptions(queryOptions);

        } else {
            throw new IOException("No applicable request decoder for resource: " + pathList.toString());
        }
    }
}
