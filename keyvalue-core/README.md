Key/value API
========================================================

The idea is to create a generic interface for storing data, that allows implementations be 100% interchangable.


Features
========

Generic Key/Value API with base API, and with a collection of extended interfaces:
- Batch writing
- CAS support
- Key iteration

Known implementations
=====================
- [keyvalue-mysql](http://github.com/ethlo/keyvalue-mysql)

Build status
============
[![Build Status](https://travis-ci.org/ethlo/keyvalue-api.png?branch=master)](https://travis-ci.org/ethlo/keyvalue-api)

Maven
=====

Repository: http://ethlo.com/maven

```
<dependency>
	<groupId>com.ethlo.keyvalue</groupId>
	<artifactId>keyvalue-api</artifactId>
	<version>0.5</version>
</dependency>
```

TODO:
=====
- Search
- Serialization
- Compression
- Test suite
