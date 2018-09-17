module systems.cauldron.drivers.lake {

    //logging
    requires slf4j.api;

    //SQL engine
    requires java.sql;
    requires calcite.core;
    requires calcite.linq4j;

    //JSON tools
    requires java.json;

    //flat-file parsing
    requires univocity.parsers;

    //cloud storage
    requires sdk.core;
    requires s3;

}