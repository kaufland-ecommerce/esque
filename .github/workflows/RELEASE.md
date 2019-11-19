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
##### 5. Commit the changes 
```shell script
VERSION="$(sed -n -E "s/^version = \"(.+)\"/\1/p" pyproject.toml)"
git add .
git commit -m "Version changed to v${VERSION}"
```
##### 6. Push the changes
```shell script
git push
```
##### 7. Open pull request to `master` on `https://github.com/real-digital/esque`
##### 8. Once the pull request is merged, create and publish a release. This should trigger a `publish` event that will, in turn, trigger publishing the package to `pypi`
