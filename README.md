
Provide some features to work with rabbitmq.

Influenced by https://github.com/williamsjj/txamqp-helpers and own private library.


## Release notes:

#### 0.2.1 and 0.2.2

* Various fixes

#### 0.2.0

* Add support for msgpack
* Various fixes for content-type support

#### 0.1.9

* Support for content-type based encoding
* Add new options for handling encoding: skip_encoding and skip_decoding
* Remove TimeoutDeferredQueue usage
* Fix delivery_mode. Change default delivery_mode to 1

#### 0.1.8

* Remove hard cjson dependency

#### 0.1.6 and 0.1.7

* Fixes

#### 0.1.5

* Extend tid support handling

#### 0.1.3

* Add NO_REPLY support for SynChan
* Add support for bindings declarations

