# check that the varchar column has been resized

new_length=$(query "select character_maximum_length as c from information_schema.columns where table_name='long-string' and column_name='id';" | jq -c '.rows[0].c | values')

if [[ "$new_length" != "257" ]]; then
	echo "id column of long-string table did not have expected length of 257, instead it had $new_length"
	exit 1
fi
