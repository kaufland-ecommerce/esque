### How to create a new release

Please follow these steps to create a new `esque` release:

##### 1. Checkout branch `release-prep`
```shell script
git checkout release-prep
# or
git checkout -b release-prep
```
##### 2. Merge the current `master` branch into it (NOTE: the code changes are already merged into the `master` branch at this point)
```shell script
git checkout master
git pull
git checkout release-prep
git merge master
```
##### 3. Run `poetry version` with the appropriate argument (`major`, `minor`, `patch`) to bump the current version
```shell script
poetry version minor
```
##### 4. Update the `CHANGELOG.md` file
##### 5. Add a tag containing the version number **prefixed with v**
```shell script
VERSION="$(sed -n -E "s/^version = \"(.+)\"/\1/p" pyproject.toml)"
git tag "v${VERSION}"
```
##### 6. Push the changes
```shell script
git commit -m "Version change to v${VERSION}"
git push
```
##### 7. Open pull request to `master`
