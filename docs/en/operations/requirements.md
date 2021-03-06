# Requirements

## CPU

In case of installation from prebuilt deb-packages use CPU with x86/64 architecture and SSE 4.2 instructions support. If you build ClickHouse from sources, you can use other processors.  

ClickHouse implements parallel data processing and uses all the hardware resources available. When choosing a processor, take into account that ClickHouse works more efficient at configurations with a large number of cores but lower clock rate than at configurations with fewer cores and a higher clock rate. For example, 16 cores with 2600 MHz is preferable than 8 cores with 3600 MHz.

Use of **Turbo Boost** and **hyper-threading** technologies is recommended. It significantly improves performance with a typical load.

## RAM

We recommend to use 4GB of RAM as minimum to be able to perform non-trivial queries. The ClickHouse server can run with a much smaller amount of RAM, but it requires memory for queries processing.

The required volume of RAM depends on:

 - The complexity of queries.
 - Amount of the data in queries.

To calculate the required volume of RAM, you should estimate the size of temporary data for [GROUP BY](../query_language/select.md#select-group-by-clause), [DISTINCT](../query_language/select.md#select-distinct), [JOIN](../query_language/select.md#select-join) and other operations you use.

ClickHouse can use external memory for temporary data. See [GROUP BY in External Memory](../query_language/select.md#select-group-by-in-external-memory) for details.

## Swap File

Disable the swap file for production environments.

## Storage Subsystem

You need to have 2GB of free disk space to install ClickHouse.

The volume of storage required for your data should be calculated separately. Assessment should include:

- Estimation of a data volume.

    You can take the sample of the data and get the size of a row from it. Then multiply the size of the row with a number of rows you plan to store.

- Data compression coefficient.

    To estimate the data compression coefficient, load some sample of your data into ClickHouse and compare the actual size of the data with the size of the table stored. For example, the typical compression coefficient for clickstream data lays in a range of 6-10 times.

To calculate the final volume of data to be stored, divide the estimated data volume by the compression coefficient.

## Network

If possible, use a 10G network.

A bandwidth of the network is critical for processing of distributed queries with a large amount of intermediate data. Also, network speed affects replication processes.

## Software

ClickHouse is developed for Linux family of operating systems. The recommended Linux distribution is Ubuntu. The `tzdata` package should be installed in the system. Name and version of an operating system where ClickHouse runs depend on the method of installation. See details in [Getting started](../getting_started/index.md) section of the documentation.
