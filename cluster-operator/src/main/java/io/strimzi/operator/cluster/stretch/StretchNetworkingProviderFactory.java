/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.InvalidConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Factory for creating StretchNetworkingProvider instances.
 *
 * <p>Supports two types of providers:</p>
 * <ul>
 *   <li><b>Built-in:</b> nodeport, loadbalancer (always available)</li>
 *   <li><b>Custom:</b> External plugins loaded via className + classPath</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <pre>{@code
 * # Built-in provider
 * STRIMZI_STRETCH_NETWORK_PROVIDER=nodeport
 *
 * # Custom plugin
 * STRIMZI_STRETCH_NETWORK_PROVIDER=custom
 * STRIMZI_STRETCH_PLUGIN_CLASS_NAME=com.example.MyProvider
 * STRIMZI_STRETCH_PLUGIN_CLASS_PATH=/opt/strimzi/plugins/*
 * }</pre>
 *
 * @since 0.47.0
 */
public final class StretchNetworkingProviderFactory {

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER =
            LogManager.getLogger(StretchNetworkingProviderFactory.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private StretchNetworkingProviderFactory() {
        // Utility class
    }

    /**
     * Create a networking provider based on configuration.
     *
     * @param operatorConfig Cluster operator configuration
     * @param config Provider-specific configuration
     * @param centralSupplier ResourceOperatorSupplier for central cluster
     * @param remoteResourceOperatorSupplier Supplier for remote cluster operators
     * @return Initialized provider
     * @throws InvalidConfigurationException if configuration is invalid
     */
    public static StretchNetworkingProvider create(
            final ClusterOperatorConfig operatorConfig,
            final Map<String, String> config,
            final ResourceOperatorSupplier centralSupplier,
            final RemoteResourceOperatorSupplier remoteResourceOperatorSupplier) {

        String providerName = operatorConfig.getStretchNetworkProvider();
        if (providerName == null || providerName.isEmpty()) {
            providerName = "nodeport";  // Default
            LOGGER.info("No stretch network provider specified, using default: nodeport");
        }

        return create(providerName, operatorConfig, config, centralSupplier, remoteResourceOperatorSupplier);
    }

    /**
     * Create a networking provider based on the provider name.
     *
     * @param providerName Provider name ("nodeport", "loadbalancer", "mcs", or "custom")
     * @param operatorConfig Cluster operator configuration
     * @param config Provider-specific configuration
     * @param centralSupplier ResourceOperatorSupplier for central cluster
     * @param remoteResourceOperatorSupplier Supplier for remote cluster operators
     * @return Initialized provider
     * @throws InvalidConfigurationException if provider name is invalid
     */
    private static StretchNetworkingProvider create(
            final String providerName,
            final ClusterOperatorConfig operatorConfig,
            final Map<String, String> config,
            final ResourceOperatorSupplier centralSupplier,
            final RemoteResourceOperatorSupplier remoteResourceOperatorSupplier) {

        LOGGER.info("Creating stretch networking provider: {}", providerName);

        StretchNetworkingProvider provider;
        String providerLower = providerName.toLowerCase(Locale.ROOT);

        switch (providerLower) {
            case "nodeport":
                LOGGER.info("Using built-in NodePort networking provider");
                provider = new NodePortNetworkingProvider();
                break;

            case "loadbalancer":
            case "lb":
                LOGGER.info("Using built-in LoadBalancer networking provider");
                provider = new LoadBalancerNetworkingProvider();
                break;

            case "custom":
                String className = operatorConfig.getStretchPluginClassName();
                String classPath = operatorConfig.getStretchPluginClassPath();

                if (className == null || className.isEmpty()) {
                    throw new InvalidConfigurationException(
                        "Custom stretch network provider requires STRIMZI_STRETCH_PLUGIN_CLASS_NAME. " +
                        "Example: STRIMZI_STRETCH_PLUGIN_CLASS_NAME=com.example.MyNetworkingProvider");
                }

                LOGGER.info("Loading custom networking provider: {} from {}",
                           className, classPath != null ? classPath : "default classpath");
                provider = loadCustomProvider(className, classPath);
                break;

            default:
                throw new InvalidConfigurationException(
                    "Unknown stretch network provider: '" + providerName + "'. " +
                    "Supported: nodeport, loadbalancer, custom");
        }

        // Initialize the provider
        try {
            provider.init(config, centralSupplier, remoteResourceOperatorSupplier)
                    .toCompletionStage().toCompletableFuture().get();
            LOGGER.info("Provider '{}' initialized successfully",
                    provider.getProviderName());
        } catch (Exception e) {
            throw new InvalidConfigurationException(
                    "Failed to initialize provider '" + providerName + "': " + e.getMessage(), e);
        }

        return provider;
    }

    /**
     * Load a custom provider by class name with optional custom classpath.
     *
     * @param className Fully qualified class name
     * @param classPath Optional classpath (supports wildcards like /opt/plugins/*)
     * @return Provider instance
     * @throws InvalidConfigurationException if class cannot be loaded or instantiated
     */
    private static StretchNetworkingProvider loadCustomProvider(
            final String className,
            final String classPath) {
        try {
            // Create custom ClassLoader if classPath is provided
            ClassLoader classLoader;
            if (classPath != null && !classPath.isEmpty()) {
                URL[] urls = parseClassPath(classPath);
                LOGGER.debug("Creating URLClassLoader with {} URLs", urls.length);
                classLoader = new URLClassLoader(urls, StretchNetworkingProviderFactory.class.getClassLoader());
            } else {
                // Use default classpath
                classLoader = StretchNetworkingProviderFactory.class.getClassLoader();
            }

            // Load the class
            Class<?> providerClass = classLoader.loadClass(className);
            LOGGER.debug("Loaded class: {}", providerClass.getName());

            // Verify it implements the interface
            if (!StretchNetworkingProvider.class.isAssignableFrom(providerClass)) {
                throw new InvalidConfigurationException(
                    "Class '" + className + "' does not implement StretchNetworkingProvider interface");
            }

            // Instantiate (requires no-arg constructor)
            StretchNetworkingProvider provider =
                (StretchNetworkingProvider) providerClass.getDeclaredConstructor().newInstance();

            LOGGER.info("Successfully loaded custom provider: {} ({})",
                       provider.getProviderName(), className);

            return provider;

        } catch (ClassNotFoundException e) {
            throw new InvalidConfigurationException(
                    "Custom networking provider class not found: '" + className + "'. " +
                    "Make sure the plugin JAR is in the classpath or specify STRIMZI_STRETCH_PLUGIN_CLASS_PATH", e);
        } catch (NoSuchMethodException e) {
            throw new InvalidConfigurationException(
                    "Custom networking provider '" + className + "' must have a public no-argument constructor", e);
        } catch (Exception e) {
            throw new InvalidConfigurationException(
                    "Failed to load custom networking provider '" + className + "': " + e.getMessage(), e);
        }
    }

    /**
     * Parse classPath string to URL array.
     *
     * <p>Supports:</p>
     * <ul>
     *   <li>Single JAR: {@code /opt/strimzi/plugins/my-plugin.jar}</li>
     *   <li>Directory with wildcard: {@code /opt/strimzi/plugins/*}</li>
     *   <li>Multiple paths: {@code /path1/plugin.jar:/path2/*}</li>
     * </ul>
     *
     * @param classPath ClassPath string
     * @return Array of URLs
     * @throws InvalidConfigurationException if classPath is invalid
     */
    private static URL[] parseClassPath(String classPath) {
        List<URL> urls = new ArrayList<>();

        try {
            for (String path : classPath.split(":")) {
                path = path.trim();

                if (path.isEmpty()) {
                    continue;
                }

                if (path.endsWith("/*")) {
                    // Handle wildcard - add all JARs in directory
                    String dirPath = path.substring(0, path.length() - 2);
                    File dir = new File(dirPath);

                    if (!dir.exists()) {
                        LOGGER.warn("Plugin directory does not exist: {}", dirPath);
                        continue;
                    }

                    if (!dir.isDirectory()) {
                        LOGGER.warn("Plugin path is not a directory: {}", dirPath);
                        continue;
                    }

                    File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
                    if (jars != null && jars.length > 0) {
                        for (File jar : jars) {
                            urls.add(jar.toURI().toURL());
                            LOGGER.debug("Added plugin JAR to classpath: {}", jar.getAbsolutePath());
                        }
                    } else {
                        LOGGER.warn("No JAR files found in plugin directory: {}", dirPath);
                    }
                } else {
                    // Single file or directory
                    File file = new File(path);
                    if (file.exists()) {
                        urls.add(file.toURI().toURL());
                        LOGGER.debug("Added to classpath: {}", file.getAbsolutePath());
                    } else {
                        LOGGER.warn("Plugin path does not exist: {}", path);
                    }
                }
            }

            if (urls.isEmpty()) {
                throw new InvalidConfigurationException(
                    "No valid plugin JARs found in classPath: " + classPath);
            }

        } catch (MalformedURLException e) {
            throw new InvalidConfigurationException(
                    "Failed to parse plugin classPath: " + classPath, e);
        }

        return urls.toArray(new URL[0]);
    }

}

