# Pure functions for provider API interaction live here; `add-stream` fills them in.
#
# Linear exposes a single GraphQL endpoint. Every stream POSTs a query document to
# this URL rather than hitting per-resource REST paths, so there are no path
# segments to append — only differing GraphQL query bodies.
API = "https://api.linear.app/graphql"
