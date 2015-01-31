DCOS Package CLI
================
CLI for working with DCOS software packages and registries.

Install
-------

#. Configure package sources and local cache location::

    dcos config package.sources ["http://dcos-registry.my.org"]
    dcos config package.cache "/var/dcos/registry"

#. Update the local package cache::

    dcos package update

#. _OPTIONAL_ Search for your desired package::

    dcos package search <query>

#. Install the desired package::

    dcos package install <package_name>
