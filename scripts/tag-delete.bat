cd ..
set TAG_NAME=v0.0.0

git tag -d %TAG_NAME%
git push origin :refs/tags/%TAG_NAME%
pause