#!/bin/bash
# Modified from https://stackoverflow.com/a/30386041

git-merge-subpath() {
    if (( $# != 6 )); then
        local PARAMS="REMOTE SOURCE_REF SOURCE_PREFIX DEST_PREFIX LICENSE_ID SOURCE_LICENSE_PATH"
        echo "USAGE: $(basename "$0") $PARAMS"
        return 1
    fi

    # Friendly parameter names; strip any trailing slashes from prefixes.
    local REMOTE="$1" SOURCE_COMMIT_INPUT="$2" SOURCE_PREFIX="${3%/}" DEST_PREFIX="${4%/}" LICENSE_ID="$5" SOURCE_LICENSE_PATH="$6"

    local SOURCE_COMMIT
    SOURCE_COMMIT=$SOURCE_COMMIT_INPUT
    local SOURCE_REMOTE_URL
    SOURCE_REMOTE_URL=$(git remote get-url "$REMOTE") || return 1

    local SOURCE_SHA1
    if ! SOURCE_SHA1=$(git rev-parse --verify "$SOURCE_COMMIT^{commit}" 2> /dev/null) ;
    then
        SOURCE_SHA1=$(git rev-parse --verify "$REMOTE/$SOURCE_COMMIT^{commit}" 2> /dev/null) || \
            echo "Unrecognized commit $REMOTE/$SOURCE_COMMIT" && return 1
        SOURCE_COMMIT="$REMOTE/$SOURCE_COMMIT"
    fi

    LICENSE_FOUND=$(\
        curl https://raw.githubusercontent.com/spdx/license-list-data/main/json/licenses.json 2> /dev/null | \
        jq --arg LICENSE "$LICENSE_ID" -r '.licenses | any(.licenseId == $LICENSE)'\
    )

    if [ "$LICENSE_FOUND" == "false" ]
    then
        echo "Error: Unrecognized license $LICENSE_ID"
        return 1
    fi

    if ! git cat-file -e "$SOURCE_COMMIT":"$SOURCE_LICENSE_PATH";
    then
        echo "Error: Unable to locate license file in $REMOTE at $SOURCE_COMMIT:$SOURCE_LICENSE_PATH"
        return 1
    fi

    local OLD_SHA1
    local GIT_ROOT
    GIT_ROOT=$(git rev-parse --show-toplevel)
    if [[ -n "$(ls -A "$GIT_ROOT/$DEST_PREFIX" 2> /dev/null)" ]]; then
        # OLD_SHA1 will remain empty if there is no match.
        local RE="^${FUNCNAME[0]}: [0-9a-f]{40} $SOURCE_PREFIX $DEST_PREFIX\$"
        OLD_SHA1=$(git log -1 --format=%b -E --grep="$RE" \
                   | grep --color=never -E "$RE" | tail -1 | awk '{print $2}')
    fi

    local OLD_TREEISH
    if [[ -n $OLD_SHA1 ]]; then
        OLD_TREEISH="$OLD_SHA1:$SOURCE_PREFIX"
    else
        # This is the first time git-merge-subpath is run, so diff against the
        # empty commit instead of the last commit created by git-merge-subpath.
        OLD_TREEISH=$(git hash-object -t tree /dev/null)
    fi &&

    git diff --color=never "$OLD_TREEISH" "$SOURCE_COMMIT:$SOURCE_PREFIX" \
        | git apply -3 --directory="$DEST_PREFIX" || git mergetool

    if (( $? == 1 )); then
        echo "Uh-oh! Try cleaning up with |git reset --merge|."
    else
        git commit -em "import: $DEST_PREFIX through $REMOTE @ $SOURCE_COMMIT_INPUT 
Remote Repo URL: $SOURCE_REMOTE_URL
Source name: $SOURCE_COMMIT
Source Commit ID: $SOURCE_SHA1
Source Repo Prefix: $SOURCE_PREFIX/
Import Path: $DEST_PREFIX/
License Type: $LICENSE_ID
License Path: $SOURCE_LICENSE_PATH

# Feel free to edit the title and body above, but make sure to keep the
# ${FUNCNAME[0]}: line below intact, so ${FUNCNAME[0]} can find it
# again when grepping git log.
${FUNCNAME[0]}: $SOURCE_SHA1 $SOURCE_PREFIX $DEST_PREFIX"
    fi
}

git-merge-subpath "$@"