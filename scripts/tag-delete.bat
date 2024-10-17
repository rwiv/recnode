cd ..
set TAG_NAME=v0.1.0

git tag -d %TAG_NAME%
git push origin :refs/tags/%TAG_NAME%
pause