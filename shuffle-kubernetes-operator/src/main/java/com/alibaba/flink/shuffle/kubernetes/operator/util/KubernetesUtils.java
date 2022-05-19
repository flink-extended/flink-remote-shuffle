/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.kubernetes.operator.util;

import com.alibaba.flink.shuffle.common.config.ConfigOption;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.functions.RunnableWithException;
import com.alibaba.flink.shuffle.common.functions.SupplierWithException;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Common utils for Kubernetes. */
public class KubernetesUtils {

    public static final String SHUFFLE_WORKER_CONTAINER_NAME = "shuffleworker";
    public static final String SHUFFLE_MANAGER_CONTAINER_NAME = "shufflemanager";
    private static final int MAX_RETRY_TIMES = 10;
    private static final long RETRY_INTERVAL = 100L;
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

    /**
     * Get resource requirements from memory and cpu.
     *
     * @param mem Memory in mb.
     * @param cpu cpu.
     * @return KubernetesResource requirements.
     */
    public static ResourceRequirements getResourceRequirements(int mem, double cpu) {
        final Quantity cpuQuantity = new Quantity(String.valueOf(cpu));
        final Quantity memQuantity = new Quantity(mem + Constants.RESOURCE_UNIT_MB);

        ResourceRequirementsBuilder resourceRequirementsBuilder =
                new ResourceRequirementsBuilder()
                        .addToRequests(Constants.RESOURCE_NAME_MEMORY, memQuantity)
                        .addToRequests(Constants.RESOURCE_NAME_CPU, cpuQuantity)
                        .addToLimits(Constants.RESOURCE_NAME_MEMORY, memQuantity)
                        .addToLimits(Constants.RESOURCE_NAME_CPU, cpuQuantity);

        return resourceRequirementsBuilder.build();
    }

    /**
     * Get the common labels for remote shuffle service clusters. All the Kubernetes resources will
     * be set with these labels.
     *
     * @param clusterId cluster id
     * @return Return common labels map
     */
    public static Map<String, String> getCommonLabels(String clusterId) {
        final Map<String, String> commonLabels = new HashMap<>();
        commonLabels.put(Constants.LABEL_APPTYPE_KEY, Constants.LABEL_APPTYPE_VALUE);
        commonLabels.put(Constants.LABEL_APP_KEY, clusterId);

        return commonLabels;
    }

    public static String getShuffleManagerNameWithClusterId(String clusterId) {
        return clusterId + "-" + SHUFFLE_MANAGER_CONTAINER_NAME;
    }

    public static String getShuffleWorkersNameWithClusterId(String clusterId) {
        return clusterId + "-" + SHUFFLE_WORKER_CONTAINER_NAME;
    }

    public static void setOwnerReference(HasMetadata resource, HasMetadata owner) {
        final OwnerReference ownerReference =
                new OwnerReferenceBuilder()
                        .withName(owner.getMetadata().getName())
                        .withApiVersion(owner.getApiVersion())
                        .withUid(owner.getMetadata().getUid())
                        .withKind(owner.getKind())
                        .withController(true)
                        .build();

        resource.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
    }

    public static OwnerReference getControllerOf(HasMetadata resource) {
        List<OwnerReference> ownerReferences = resource.getMetadata().getOwnerReferences();
        for (OwnerReference ownerReference : ownerReferences) {
            if (ownerReference.getController().equals(Boolean.TRUE)) {
                return ownerReference;
            }
        }
        return null;
    }

    public static String getResourceFullName(HasMetadata resource) {
        return getNameWithNameSpace(
                resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    public static String getNameWithNameSpace(String namespace, String name) {
        return namespace + "/" + name;
    }

    public static void executeWithRetry(RunnableWithException action, String actionName) {
        for (int retry = 0; retry < MAX_RETRY_TIMES; retry++) {
            try {
                action.run();
                return;
            } catch (Throwable throwable) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            String.format("%s error, retry times = %d.", actionName, retry),
                            throwable);
                }
                if (retry >= MAX_RETRY_TIMES - 1) {
                    throw new RuntimeException(String.format("%s failed.", actionName), throwable);
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Extract and parse configuration properties with a given name prefix and return the result as
     * a Map.
     */
    public static Map<String, String> getPrefixedKeyValuePairs(
            String prefix, Configuration configuration) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                String key = entry.getKey().substring(prefix.length());
                result.put(key, entry.getValue());
            }
        }
        return result;
    }

    public static void updateResourceRequirements(
            ResourceRequirements resourceRequirements, Map<String, String> limitFactors) {

        limitFactors.forEach(
                (type, factor) -> {
                    final Quantity quantity = resourceRequirements.getRequests().get(type);
                    if (quantity != null) {
                        final double limit =
                                Double.parseDouble(quantity.getAmount())
                                        * Double.parseDouble(factor);
                        LOG.info("Updating the {} limit to {}", type, limit);
                        resourceRequirements
                                .getLimits()
                                .put(
                                        type,
                                        new Quantity(String.valueOf(limit), quantity.getFormat()));
                    } else {
                        LOG.warn(
                                "Could not find the request for {}, ignoring the factor {}.",
                                type,
                                factor);
                    }
                });
    }

    public static Map<String, String> filterVolumeMountsConfigs(Map<String, String> configs) {
        return filterConfigsWithSpecifiedKeys(
                configs, Arrays.asList(Constants.VOLUME_NAME, Constants.VOLUME_MOUNT_PATH));
    }

    public static Map<String, String> filterEmptyDirVolumeConfigs(Map<String, String> configs) {
        return filterConfigsWithSpecifiedKeys(
                configs,
                Arrays.asList(
                        Constants.VOLUME_NAME,
                        Constants.EMPTY_DIR_VOLUME_MEDIUM,
                        Constants.EMPTY_DIR_VOLUME_SIZE_LIMIT));
    }

    public static Map<String, String> filterHostPathVolumeConfigs(Map<String, String> configs) {
        return filterConfigsWithSpecifiedKeys(
                configs,
                Arrays.asList(
                        Constants.VOLUME_NAME,
                        Constants.HOST_PATH_VOLUME_PATH,
                        Constants.HOST_PATH_VOLUME_TYPE));
    }

    public static List<Map<String, String>> filterVolumesConfigs(
            Configuration configuration,
            ConfigOption<List<Map<String, String>>> option,
            Function<Map<String, String>, Map<String, String>> configFilter) {
        List<Map<String, String>> volumes =
                Optional.ofNullable(configuration.getList(option, Map.class))
                        .orElse(Collections.emptyList());

        return volumes.stream().map(configFilter).collect(Collectors.toList());
    }

    private static Map<String, String> filterConfigsWithSpecifiedKeys(
            Map<String, String> configs, List<String> specifiedKeys) {
        return configs.entrySet().stream()
                .filter(entry -> specifiedKeys.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition, Duration timeout)
            throws Exception {
        waitUntilCondition(
                condition, timeout, RETRY_INTERVAL, "Condition was not met in given timeout.");
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            Duration timeout,
            long retryIntervalMillis,
            String errorMsg)
            throws Exception {
        long timeLeft = timeout.toMillis();
        long endTime = System.currentTimeMillis() + timeLeft;

        while (timeLeft > 0 && !condition.get()) {
            Thread.sleep(Math.min(retryIntervalMillis, timeLeft));
            timeLeft = endTime - System.currentTimeMillis();
        }

        if (timeLeft <= 0) {
            throw new TimeoutException(errorMsg);
        }
    }
}
