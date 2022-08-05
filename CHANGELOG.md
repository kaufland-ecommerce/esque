### Version 0.5.1

Simple bug fix in stdout, where `from_context` was used instead of `to_context`

### Version 0.5.0
Dependency upgrades:
* attrs 1.4.0 -> 1.4.1
* avro-python3 1.10.0 -> 1.10.2
* bandit 1.7.0 -> 1.7.4
* Black 19.3b0 -> 22.6.0 (update needed to be compatible with click >=8.0.0)
* certifi 2021.5.30 -> 2022.6.15
* charset-normalizer 2.0.4 -> 2.1.0
* click 7.1.2 -> 8.1.3 (autocompletion parameter changed to shell_complete)
* colorama 0.4.4 -> 0.4.5
* confluent-kafka 1.7.0 -> 1.9.0
* decopath 1.4.8 -> 1.4.10
* fastavro 1.4.4 -> 1.5.4
* gitdb 4.0.7 -> 4.0.9
* gitpython 3.1.20 -> 3.1.27
* idea 3.2 -> 3.3
* makefun 1.11.3 -> 1.14.0
* more-itertools 8.10.0 -> 8.13.0
* packaging 21.0 -> 21.3
* pbr 5.6.0 -> 5.9.0
* py 1.10.0 -> 1.11.0
* pyparsing 2.4.7 -> 3.0.9
* pytest-cases 3.6.4 -> 3.6.13
* requests 2.26.0 -> 2.28.1
* smmap 4.0.0 -> 5.0.0
* stevedore 3.3.0 -> 4.0.0
* typing-extensions 3.10.0.0 -> 4.3.0
* urllib3 1.26.6 -> 1.26.11

### Version 0.4.1
* Extended the timeout when fetching cluster metadata to 20 seconds
* Extended the timeout when creating the python-kafka client to 30 seconds

### Version 0.4.0
* Introduced new `io` module to replace old consumer logic
* Restructured the `cli.commands` module into sub packages/modules for better overview
* Removed old client/consumer logic
* Lazily instantiate admin clients to improve responsiveness of some commands
* Reintroduced the `esque transfer` command to transfer data from one topic to another
* Added the `esque io` command for more advanced use cases that are not covered by the options provided with the 
  `esque consume`, `esque produce` and `esque transfer` commands.
* Added more information to `esque create topic` to show you what will be created before you approve it.
* Added more information to `esque ping` to also show timings from client to server and from server to client.

#### Breaking changes
`esque consume --avro --stdout` now json serializes the avro data into the message key and value objects which in turn 
are always strings. Therefore, attributes of key or value objects will not be directly accessible on the root structure.
However, as a workaround, you could for example pipe the data through `jq '.key | fromjson'` to decode the json objects 
again.

`esque ping` now does not create a new topic all the time but uses a topic called `esque-ping` with random message 
key to distinguish ping messages from different users.

The files and file structure in the directories that were created by `esque consume` and required by `esque produce` 
has changed. Directories that were previously created with `esque consume` won't work with `esque produce` anymore.

### Version 0.3.1
* Added the `delete topics` command that enables deleting more than one topic
* Modified the existing `delete topic` command to accept only a single topic entry
* Refactored the topic deletion logic to improve performance

### Version 0.3.0
* Remove pykafka dependency
* Include a partial fix for #32 (topics are now displayed, output is still not optimal)
* Minor performance improvements

### Version 0.2.10
* Bugfix for issue #175
* Do not fail silently upon offset commit error
* Update confluent_kafka dependency in order to fix timeout issues when committing offsets to an SSL secured Kafka 
  endpoint
* Running esque now creates a cProfiler snapshot when the environment variable `ESQUE_PROFILE` is set to a non-empty 
  value
* Fix issue with decimal values from avro not being properly displayed in stdout

### Version 0.2.9
* Added `--binary` flag to `produce` and `consume`

### Version 0.2.8
* Updated avro-python3 to version 1.10.0

### Version 0.2.7
* Exit with code 1 when `apply` tries to change the number of partitions or the replication factor for a topic.

### Version 0.2.6
* Enabled listing internal topics by default. Added `--hide-internal` flag to `get topics` command.

### Version 0.2.5
* Enable support for variadic arguments when deleting topics and consumer groups.
* Enable piping in arguments when deleting topics and consumer groups.

### Version 0.2.4
* Add `delete consumergroup` command.

### Version 0.2.3
* Converted unserializable values to strings when writing to STDOUT

### Version 0.2.2
* Consume option renamed from `numbers` to `number`.
* Add `edit offsets` command.
* Bugfix for `OverflowError` in `pretty_duration` function.
* use `esque-client` as default group id for all esque commands
* disable group offset commits for ping, consume and describe

### Version 0.2.1
* Bugfix for issue #154

### Version 0.2.0
* Bugfix for issue #149

### Version 0.2.0b3
* Bugfix #136: Fix wrong schema version for null payload
* Enhancement #141: Split folder for key and value schema when consuming messages
* Fix for issue #46: apply command doesn't consider cluster defaults when settings are changed
* Fix for issue #132: getting information about internal topics is not possible
* Fix for issue #143: Consume sometimes finishes successfully without writing any messages

### Version 0.2.0b2
* Enhancement #129: use offsets_for_times to set consumer group offsets
 to specific point in time
* Fix for issue #125: esque was often throwing MessageEmptyException's when operating on topics with empty partitions.

### Version 0.2.0b0
* Moved the CI workflow to GitHub Actions
* Command `edit consumergroup` renamed to `set offsets`
* Split the `transfer` command into `consume` and `produce`
* Improved the command docs and context help
* Improved error handling
* and many, many more
