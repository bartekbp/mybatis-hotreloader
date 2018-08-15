package com.github.bartekbp.mybatis;


import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.ResultMapResolver;
import org.apache.ibatis.builder.annotation.MapperAnnotationBuilder;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.session.Configuration;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RequiredArgsConstructor
@Slf4j
public class MybatisHotReloader implements InitializingBean, DisposableBean {
  @NonNull
  private Resource[] mapperLocations;
  @NonNull
  private Configuration configuration;

  private WatchService watchService;
  private ExecutorService executor;
  private Map<Path, byte[]> mostRecentFileHash = new ConcurrentHashMap<>();
  private Collection<Class<?>> mapperClasses;

  public void afterPropertiesSet() throws Exception {
    addStatementLock();

    this.mapperClasses = ImmutableList.copyOf(this.configuration.getMapperRegistry().getMappers());

    this.watchService = FileSystems.getDefault()
      .newWatchService();

    this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(getClass().getSimpleName())
      .build());

    for (Resource resource : mapperLocations) {
      Paths.get(resource.getURI())
        .getParent()
        .register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
    }

    this.executor.execute(() -> {
      while(!Thread.interrupted()) {
        this.watchFiles();
      }

      log.info("Stopped watching mappers");
    });
  }

  private Object getLock() {
    return this.configuration.getIncompleteResultMaps();
  }

  private void addStatementLock() {
    this.configuration.getIncompleteResultMaps()
      .add(new ResultMapResolver(null, null, null, null, null, null, false) {
        @Override
        @SneakyThrows
        public ResultMap resolve() {
          // sentinel forcing sqls to access getLock monitor
          return null;
        }
      });
  }

  @SneakyThrows
  private void watchFiles() {
    WatchKey watchKey = watchService.take();
    try {
      onPathChange(watchKey);
    } catch (Exception e) {
      log.error("Error while refreshing mappers", e);
    } finally {
      watchKey.reset();
    }
  }

  @SneakyThrows
  private void onPathChange(WatchKey watchKey) {
    Path baseDir = (Path) watchKey.watchable();
    for (WatchEvent<?> pollEvent : watchKey.pollEvents()) {
      Path path = (Path) pollEvent.context();
      Path changedFileAbsPath = baseDir.resolve(path).toAbsolutePath();
      byte[] lastFileHash = mostRecentFileHash.getOrDefault(changedFileAbsPath, new byte[0]);
      byte[] currentFileHash = getPathContentHash(changedFileAbsPath);

      if(Arrays.equals(lastFileHash, currentFileHash)) {
        log.info("Not reloading mapper - same file hash");
        continue;
      }

      mostRecentFileHash.put(changedFileAbsPath, currentFileHash);
      for (Resource resource : this.mapperLocations) {
        Path resourceAbsPath = Paths.get(resource.getURI()).toAbsolutePath();
        if (Objects.equals(resourceAbsPath, changedFileAbsPath)) {
          log.info("Found mapper file to reload <{}>", resource.getURI());
          this.reload(resourceAbsPath);
          return;
        }
      }
    }
  }

  private byte[] getPathContentHash(Path changedFileAbsPath) throws IOException {
    try(FileInputStream fileInputStream = new FileInputStream(changedFileAbsPath.toFile())) {
      byte[] fileContent = IOUtils.toByteArray(fileInputStream);
      return Hashing.sha1()
        .hashBytes(fileContent)
        .asBytes();
    }
  }

  private void reload(Path resourceAbsPath) throws IOException {
    this.reloadAll();
  }

  @SneakyThrows
  private void reloadAll() {
    synchronized (this.getLock()) {
      // unfortunately, we can't be sure that there are no threads that didn't synchronize on lock,
      // we only ensure that no new will start executing without lock
      Thread.sleep(100);

      this.clearOldConfiguration();

      for (Resource resource : this.mapperLocations) {
        XMLMapperBuilder xmlMapperBuilder = new XMLMapperBuilder(resource.getInputStream(),
          configuration,
          resource.toString(),
          configuration.getSqlFragments());
        xmlMapperBuilder.parse();

        // add namespace to avoid loading same xml twice - see XMLMapperBuilder.bindMapperForNamespace
        String namespace = getNamespace(xmlMapperBuilder);
        configuration.addLoadedResource("namespace:" + namespace);

        // parsing xml removes our sentinel, so we need to restore it
        addStatementLock();
      }

      for(Class<?> mapperClazz: this.mapperClasses) {
        MapperAnnotationBuilder parser = new MapperAnnotationBuilder(this.configuration, mapperClazz);
        parser.parse();
      }

      log.info("All mapper files reloaded");
    }

  }

  private String getNamespace(XMLMapperBuilder xmlMapperBuilder) {
    Field builderAssistant = ReflectionUtils.findField(XMLMapperBuilder.class, "builderAssistant");
    ReflectionUtils.makeAccessible(builderAssistant);
    MapperBuilderAssistant assistant = (MapperBuilderAssistant) ReflectionUtils.getField(builderAssistant, xmlMapperBuilder);
    return assistant.getCurrentNamespace();
  }

  private void clearOldConfiguration() throws Exception {
    this.clearFieldValue(configuration, "mappedStatements");
    this.clearFieldValue(configuration, "caches");
    this.clearFieldValue(configuration, "resultMaps");
    this.clearFieldValue(configuration, "parameterMaps");
    this.clearFieldValue(configuration, "keyGenerators");
    this.clearFieldValue(configuration, "loadedResources");
    this.clearFieldValue(configuration, "sqlFragments");
  }

  @SneakyThrows
  private void clearFieldValue(Configuration configuration, String fieldName) {
    Field field = ReflectionUtils.findField(configuration.getClass(), fieldName);
    ReflectionUtils.makeAccessible(field);

    Object fieldValue = ReflectionUtils.getField(field, configuration);
    if(fieldValue instanceof Map) {
      ((Map) fieldValue).clear();
    } else if(fieldValue instanceof Set) {
      ((Set) fieldValue).clear();
    } else {
      throw new IllegalArgumentException("Field type not supported!");
    }
  }

  @Override
  public void destroy() throws IOException {
    this.watchService.close();
    this.executor.shutdown();
  }
}

