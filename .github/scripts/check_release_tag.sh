#!/usr/bin/env bash
# Decide whether a release tag can be published.
#
#   check_release_tag.sh <tag> [<branch>]
#
# Fails on a tag that is not vX.Y.Z, on a lightweight tag, and on a tag whose
# commit is not on <branch> (default: master). Succeeds without publishing on a
# tag whose message has a "Retroactive:" line: such tags record past releases.
# Writes publish=true or publish=false to $GITHUB_OUTPUT when it is set.
set -euo pipefail

tag="${1:?usage: check_release_tag.sh <tag> [<branch>]}"
branch="${2:-master}"
output="${GITHUB_OUTPUT:-/dev/null}"

fail() {
  echo "::error::$1" >&2
  shift
  for line in "$@"; do echo "$line" >&2; done
  exit 1
}

[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "$tag is not a release tag (vX.Y.Z)."

# actions/checkout can store an annotated tag as a lightweight ref, so fetch the
# tag object itself, and the branch the tag must be on.
git fetch --quiet --force origin "refs/tags/$tag:refs/tags/$tag"
git fetch --quiet --no-tags origin "+refs/heads/$branch:refs/remotes/origin/$branch"

if [[ "$(git cat-file -t "refs/tags/$tag")" != "tag" ]]; then
  fail "$tag is a lightweight tag. A release tag is annotated, with the release notes as its message:" \
    "  git tag -d $tag" \
    "  git tag -a $tag <commit> --cleanup=verbatim -F notes.md" \
    "  git push --force origin refs/tags/$tag"
fi

commit="$(git rev-parse "refs/tags/$tag^{commit}")"
if ! git merge-base --is-ancestor "$commit" "refs/remotes/origin/$branch"; then
  fail "$tag points to $commit, which is not on $branch."
fi

if git for-each-ref "refs/tags/$tag" --format='%(contents)' | grep -q '^Retroactive:'; then
  echo "::notice::$tag records a past release (its message has a Retroactive: line); nothing to publish."
  echo "publish=false" >> "$output"
  exit 0
fi

echo "$tag is an annotated tag on $branch; publishing."
echo "publish=true" >> "$output"
