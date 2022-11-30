keyvalue-mysql
==============

MySQL 5.6 introduced a key/value (memcached) API for faster data storage/reads (avoiding overhead of SQL).

NOTE: Also contains a legacy connector with the same API to work with older versions of MySQL, using regular SQL.

This project utilises the ethlo [Key/Value API](https://github.com/ethlo/keyvalue-api).

Build status
============
[![Build Status](https://travis-ci.org/ethlo/keyvalue-mysql.png?branch=master)](https://travis-ci.org/ethlo/keyvalue-mysql)

Maven
=====

Repository: http://ethlo.com/maven

```
<dependency>
	<groupId>com.ethlo.keyvalue</groupId>
	<artifactId>keyvalue-mysql</artifactId>
	<version>0.6</version>
</dependency>
```
