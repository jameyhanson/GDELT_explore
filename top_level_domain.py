# top_level_domain.py

# tld- return the top level domain
@outputSchema("tld:chararray")
def tld(host):
    return host[host.rfind('.')+1:]
