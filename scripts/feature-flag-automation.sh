#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(dirname "$0")"

# Check for required args
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 FLAG_NAME CONNECTOR [CONNECTOR...]"
  echo "Example: $0 no_frob_xyzzy source-mysql source-postgres"
  exit 1
fi

FLAG=$1
shift
CONNECTORS=("$@")
WORKDIR="specs_${FLAG}_$(date +%Y%m%d_%H%M%S)"

# Show plan and get user confirmation
echo "Setting flag '$FLAG' on connectors: ${CONNECTORS[*]}"
echo "Working directory: $WORKDIR"
echo "Plan:"
echo ""
for CONNECTOR in "${CONNECTORS[@]}"; do
  echo "    $SCRIPT_DIR/list-tasks.sh --connector=$CONNECTOR --pull --missing=$FLAG --dir=$WORKDIR"
done
echo "    $SCRIPT_DIR/bulk-config-editor.sh --set_flag=$FLAG --dir=$WORKDIR"
echo "    $SCRIPT_DIR/bulk-publish.sh --mark --dir=$WORKDIR"
echo

read -p "Proceed? (y/N) " REPLY
if [[ ! $REPLY == "y" && ! $REPLY == "Y" ]]; then
  echo "Operation cancelled"
  exit 1
fi

# Create working directory
mkdir -p "$WORKDIR"

# List and pull tasks for each connector name provided
for CONNECTOR in "${CONNECTORS[@]}"; do
  echo "Listing and pulling tasks missing $FLAG for $CONNECTOR..."
  "$SCRIPT_DIR/list-tasks.sh" --connector="$CONNECTOR" --pull --missing="$FLAG" --dir="$WORKDIR"
done

echo "Adding feature flag setting $FLAG to all configs..."
"$SCRIPT_DIR/bulk-config-editor.sh" --set_flag="$FLAG" --dir="$WORKDIR"

echo "Publishing all modified tasks..."
"$SCRIPT_DIR/bulk-publish.sh" --mark --dir="$WORKDIR"

echo "All operations completed. Check for any tasks that failed to publish."
echo "To retry failed tasks, run this command again with the same arguments."
