### Version 0.2.0b2
* Fix for issue #125: esque was often throwing MessageEmptyException's when operating on topics with empty partitions.
### Version 0.2.0b0
* Moved the CI workflow to GitHub Actions
* Command `edit consumergroup` renamed to `set offsets`
* Split the `transfer` command into `consume` and `produce`
* Improved the command docs and context help
* Improved error handling
* and many, many more