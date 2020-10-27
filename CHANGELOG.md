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
