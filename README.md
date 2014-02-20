HQuant-State: AcidState Time Series Database for HQuant
======================================

HQuant-State is a time series database for HQuant, meant to store historical stock and option
quotes and historical balances and incomes. It is meant to be run as a separate service, and it is
expected that it will be written to by a DEQ client handling a quote stream (perhaps from
hdeq-tradeking or some other stock data provider).

HQuant-State is a hybrid in-memory/persistent storage database. Due to the nature of time series, it
is append only, meaning that it can make several optimizations that other databases can't. Most
notably, it only keeps a set amount of historical data in memory, and puts the rest into storage
somewhere else. Currently, this storage can be either the local disk or an Amazon S3 bucket. This
behavior is entirely configurable.

HQuant-State also supports the Archival and Indexing of SEC EDGAR XBRL business reports. This means
that it can be used to store historical balance sheets and income statements. Starting from 2011
onwards, the SEC requires all companies to submit electronic versions of their annual
reports. Combine with a solution such as a HDeq and HDeq-EDGAR, you can define an adapter that will
update the database whenever a new statement comes out. Once again, actual documents are stored
either on-disk or on Amazon S3.
