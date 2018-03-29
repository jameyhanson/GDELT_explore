Register 'top_level_domain.py' using jython as example_udf;

# ref. https://pig.apache.org/docs/r0.16.0/udf.html#jython-udfs

hosts = LOAD '/data/hosts/' AS (
    host:chararray
);

host_tld = FOREACH hosts GENERATE (
    hosts,
    example_udf.tld(host) AS tld);
    
DESCRIBE host_tld;

host_tld = LIMIT host_tld 10;

DUMP host_tld;
