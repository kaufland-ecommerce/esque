### next version
* Bugfix #136: Fix wrong schema version for null payload
* Enhancement #141: Split folder for key and value schema when consuming messages
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
