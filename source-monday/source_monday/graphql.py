import asyncio
from typing import Dict, Any
from logging import Logger

from estuary_cdk.http import HTTPSession
from source_monday.models import GraphQLResponse


API = "https://api.monday.com/v2"


class GraphQLError(RuntimeError):
    """Raised when a GraphQL query returns an error."""

    def __init__(self, errors: list[dict]):
        super().__init__(f"GraphQL query returned errors. Errors: {errors}")
        self.errors = errors


async def execute_query(
    http: HTTPSession, log: Logger, query: str, variables: Dict[str, Any] = None
) -> GraphQLResponse:
    """Execute a GraphQL query and return the response."""

    response = GraphQLResponse.model_validate_json(
        await http.request(
            log,
            API,
            method="POST",
            json={"query": query, "variables": variables}
            if variables
            else {"query": query},
        )
    )

    if response.errors:
        raise GraphQLError(response.errors)

    return response


async def check_complexity(http: HTTPSession, log: Logger, threshold: int) -> None:
    """Check API complexity and wait if necessary."""
    data = await execute_query(http, log, COMPLEXITY)
    complexity = data["data"]["complexity"]

    if complexity["after"] < threshold:
        wait_time = complexity["reset_in_x_seconds"]
        log.warning(f"Complexity limit. Waiting for reset in {wait_time} seconds.")
        await asyncio.sleep(wait_time)


COMPLEXITY = """
query GetComplexity {
  complexity {
    before
    query
    after
    reset_in_x_seconds
  }
}
"""


ACCOUNT_FIELDS = """
fragment AccountFields on Account {
  active_members_count
  country_code
  first_day_of_the_week
  id
  logo
  name
  plan {
    max_users
    period
    tier
    version
  }
  products {
    default_workspace_id
    id
    kind
  }
  show_timeline_weekends
  sign_up_product_kind
  slug
  tier
}
"""

TEAM_FIELDS = """
fragment TeamFields on Team {
  id
  name
  owners {
    id
    # Cannot use ...UserFields - Fill on the client side (code)
  }
  picture_url
  users {
    id
    account {
      id
    }
  }
}
"""

USER_FIELDS = f"""
fragment UserFields on User {{
  birthday
  country_code
  created_at
  current_language
  email
  enabled
  id
  is_admin
  is_guest
  is_pending
  is_view_only
  is_verified
  join_date
  last_activity
  location
  mobile_phone
  name
  out_of_office {{
    active
    disable_notifications
    end_date
    start_date
    type
  }}
  phone
  photo_original
  photo_small
  photo_thumb
  photo_thumb_small
  photo_tiny
  sign_up_product_kind
  # Teams
  teams {{
    ...TeamFields
  }}
  # Accounts
  account {{
    ...AccountFields
  }}
}}
{ACCOUNT_FIELDS}
{TEAM_FIELDS}
"""

TAG_FIELDS = """
fragment TagFields on Tag {
  color
  id
  name
}
"""

ACTIVITY_LOGS_FIELDS = """
fragment ActivityLogsFields on ActivityLogType {
  account_id
  data
  entity
  event
  id
  user_id
  created_at
}
"""

COLUMN_FIELDS = """
fragment ColumnFields on Column {
  archived
  description
  id
  settings_str
  title
  type
  width
}
"""

BOARDS = f"""
query GetBoards($order_by: BoardsOrderBy!, $limit: Int!, $page: Int!) {{
  boards(order_by: $order_by, limit: $limit, page: $page, state: active) {{
    board_folder_id
    board_kind
    communication
    description
    id
    item_terminology
    items_count
    name
    permissions
    state
    type
    updated_at
    url
    workspace_id
    
    # Activity Logs
    activity_logs {{
      ...ActivityLogsFields
    }}
    # Columns
    columns {{
      ...ColumnFields
    }}
    # Users
    creator {{
      ...UserFields
    }}
    # Groups
    groups {{
      id
      title
      archived
      color
      items_page {{
        cursor
        # Items
        items {{
          id
        }}
      }}
      position
    }}
    items_page {{
      cursor
      # Items
      items {{
        id
      }}
    }}
    # Users
    owners {{
      ...UserFields
    }}
    # Users
    subscribers {{
      ...UserFields
    }}
    tags {{
      id
      color
      name
    }}
    # Teams
    team_owners {{
      id
      # Users
      owners {{
        id
      }}
    }}
    # Teams
    team_subscribers
    {{
      id
      # Users
      owners {{
        id
      }}
    }}
    top_group {{
      id
      title
      archived
      color
      items_page {{
        cursor
        # Items
        items {{
          id
        }}
      }}
    }}
    # Updates
    updates {{
      id
    }}
    # Board Views
    views {{
      id
    }}
    # Workspace
    workspace {{
      id
    }}
  }}
}}
{USER_FIELDS}
{COLUMN_FIELDS}
{ACTIVITY_LOGS_FIELDS}
"""

TEAMS = f"""
query GetTeams {{
  teams {{
    ...TeamFields
  }}
}}
{TEAM_FIELDS}
"""

USERS = f"""
query GetUsers {{
  users {{
    ...UserFields
  }}
}}
{USER_FIELDS}
"""

ITEMS_INITIAL = """
query GetItems(
  $boards_order_by: BoardsOrderBy!,
  $boards_limit: Int!,
  $boards_page: Int!,
  $items_limit: Int!,
  $items_order_by: String!,
  $items_order_direction: ItemsOrderByDirection!
) {
  boards(
    order_by: $boards_order_by,
    limit: $boards_limit,
    page: $boards_page,
    state: active
  ) {
    id
    name
    items_page(
      limit: $items_limit,
      query_params: {
        order_by:[{ column_id: $items_order_by, direction: $items_order_direction }]
      }
    ) {
      cursor
      items {
        id
        name
        board {
          id
          name
        }
        group {
          id
          title
        }
        column_values {
          id
          text
          value
        }
        creator {
          id
          name
        }
        created_at
        updated_at
      }
    }
  }
}
"""

NEXT_ITEMS_PAGE = """
query GetNextItemsPage($cursor: String!, $items_limit: Int!) {
  next_items_page (limit: $items_limit, cursor: $cursor) {
    cursor
    items {
      id
      name
      board {
        id
        name
      }
      group {
        id
        title
      }
      column_values {
        id
        text
        value
      }
      creator {
        id
        name
      }
      created_at
      updated_at
    }
  }
}
"""

ITEMS = """
query GetItems($order_by: BoardsOrderBy!, $limit: Int!, $page: Int!) {
  boards(order_by: $order_by, limit: $limit, page: $page, state: active) {
    items_page {
      items {
        id
        board {
          id
        }
        updates {
          id
          body
          creator_id
          edited_at
          edited_at
          creator {
            id
            account {
              id
            }
            account_products {
              default_workspace_id
              id
              kind
            }
            custom_field_metas {
              id
            }
            custom_field_values {
              custom_field_meta_id
              value
            }
            teams {
              id
              owners {
                id
                account {
                  id
                  products {
                    default_workspace_id
                    id
                    kind
                  }
                }
                account_products {
                  default_workspace_id
                  id
                  kind
                }
                custom_field_metas {
                  id
                }
                custom_field_values {
                  custom_field_meta_id
                  value
                }
              }
            }
          }
          likes {
            id
            creator_id
            creator {
              id
              account {
                id
                plan {
                  max_users
                  period
                  tier
                  version
                }
                products {
                  default_workspace_id
                  id
                  kind
                }
              }
              account_products {
                default_workspace_id
                id
                kind
              }
              custom_field_metas {
                id
              }
              custom_field_values {
                custom_field_meta_id
                value
              }
              teams {
                id
                owners {
                  id
                  account {
                    id
                    products {
                      default_workspace_id
                      id
                      kind
                    }
                  }
                  account_products {
                    default_workspace_id
                    id
                    kind
                  }
                  custom_field_metas {
                    id
                  }
                  custom_field_values {
                    custom_field_meta_id
                    value
                  }
                }
              }
            }
          }
          pinned_to_top {
            item_id
          }
          watchers {
            user_id
            medium
            user {
              id
              account {
                active_members_count
                country_code
                first_day_of_the_week
                id
                logo
                name
                plan {
                  max_users
                  period
                  tier
                  version
                }
                products {
                  default_workspace_id
                  id
                  kind
                }
                show_timeline_weekends
                sign_up_product_kind
                slug
                tier
              }
              account_products {
                default_workspace_id
                id
                kind
              }
              birthday
              country_code
              created_at
              current_language
              custom_field_metas {
                description
                editable
                field_type
                flagged
                icon
                id
                position
                title
              }
              custom_field_values {
                custom_field_meta_id
                value
              }
              email
              enabled
              is_admin
              is_guest
              is_pending
              is_verified
              is_verified
              is_view_only
              join_date
              last_activity
              location
              mobile_phone
              name
              out_of_office {
                active
                disable_notifications
                end_date
                start_date
              }
              phone
              photo_original
              photo_small
              photo_thumb
              photo_thumb_small
              photo_tiny
              sign_up_product_kind
              teams {
                id
                owners {
                  id
                  account {
                    active_members_count
                    country_code
                    first_day_of_the_week
                    id
                    logo
                    name
                    plan {
                      max_users
                      period
                      tier
                      version
                    }
                    products {
                      default_workspace_id
                      id
                      kind
                    }
                    show_timeline_weekends
                    sign_up_product_kind
                    slug
                    tier
                  }
                  account_products {
                    default_workspace_id
                    id
                    kind
                  }
                  birthday
                  country_code
                  created_at
                  current_language
                  custom_field_metas {
                    description
                    editable
                    field_type
                    flagged
                    icon
                    id
                    position
                    title
                  }
                  custom_field_values {
                    custom_field_meta_id
                    value
                  }
                  email
                  enabled
                  is_admin
                  is_guest
                  is_pending
                  is_verified
                  is_verified
                  is_view_only
                  join_date
                  last_activity
                  location
                  mobile_phone
                  name
                  out_of_office {
                    active
                    disable_notifications
                    end_date
                    start_date
                  }
                  phone
                  photo_original
                  photo_small
                  photo_thumb
                  photo_thumb_small
                  photo_tiny
                  sign_up_product_kind
                }
              }
              time_zone_identifier
              title
              url
              utc_hours_diff
            }
          }
          created_at
          updated_at
          item_id
          item {
            id
          }
          replies {
            id
            # TODO
          }
          assets {
            id
            # TODO
          }
          text_body
        }
      }
    }
  }
}
"""

TAGS = f"""
query GetTags {{
  tags {{
    ...TagFields
  }}
}}
{TAG_FIELDS}
"""
