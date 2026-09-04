#!/bin/sh
# Smart pip install — skips packages already at the required version.
# Only installs when: package is missing OR installed version != required version.
set -e

FILE="$1"
TMPFILE=$(mktemp /tmp/pip_to_install.XXXXXX)

while IFS= read -r req; do
    # Skip blank lines and comments
    case "$req" in ''|\#*) continue ;; esac

    pkg=$(echo "$req" | sed 's/[=><!].*//' | tr -d ' ')
    req_ver=$(echo "$req" | awk -F'==' '{print $2}' | tr -d ' ')

    if [ -n "$req_ver" ]; then
        cur_ver=$(pip show "$pkg" 2>/dev/null | awk '/^Version:/{print $2}')
        if [ "$cur_ver" = "$req_ver" ]; then
            echo "  [cached]  $pkg==$req_ver"
            continue
        fi
        echo "  [install] $pkg  ${cur_ver:-not installed} -> $req_ver"
    fi

    echo "$req" >> "$TMPFILE"
done < "$FILE"

if [ -s "$TMPFILE" ]; then
    pip install --quiet -r "$TMPFILE"
    echo "  done."
else
    echo "  all packages already at required versions — nothing to install."
fi

rm -f "$TMPFILE"
