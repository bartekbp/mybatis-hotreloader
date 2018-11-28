# mybatis-hotreloader

The goal of this project is to autoreload changed mybatis mapper files. 
Current version is experimental and by no means safe to run in production.

## How to use:
In your spring configuration declare a bean:
```
  @Bean
  public MybatisHotReloader hotreloader(SqlSessionFactory sqlSessionFactory) {
    return new MybatisHotReloader(getMapperLocations(), sqlSessionFactory.getConfiguration());
  }
```
That's it.
The plugin relies on watching mybatis files, so you need to ensure that once you change mapper file, it gets copied to your build system output directory.
In case of Maven+Intellij Idea, it's as easy as forcing file compilation with ctrl+shift+f9 shortcut.
