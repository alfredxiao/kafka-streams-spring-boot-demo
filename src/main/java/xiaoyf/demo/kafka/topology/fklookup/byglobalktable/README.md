# Comments

`GlobalKTable` is backed by a source topic, which is meant to be compacted (otherwise, when application
is restarted, it reads in different set of keys due to some segments are already deleted).

`GlobalKTable` is NOT time-synchronised, read the comments in `FkLookupByGlobalKTableTopology`.

If we want to choose between in-memory or persistent, we can utilise the `Materialized` class.