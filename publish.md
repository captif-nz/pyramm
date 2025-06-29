
## Publish to PyPI

Pushing a tag with format `v*` to the remote repository will trigger a publish to PyPI.

Follow these steps:

1. Merge master branch into stable branch without checkout.

    ```
    git fetch . dev:master
    ```

2. Create a new tag.

    ```
    git tag -a v0.1 -m "initial release"
    ```

3. Push master branch and the new tag.

    ```
    git push origin master v0.1
    ```

It may be helpful to print the current tree in the command line:

```
git log --oneline --graph --color --all --decorate
```
