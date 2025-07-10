API = "https://api.monday.com/v2"
API_VERSION = "2025-07"

TAGS = """
query {
    tags {
        id
        name
        color
    }
}
"""

USERS = """
query ($limit: Int = 10, $page: Int = 1) {
    users(limit: $limit, page: $page) {
        birthday
        country_code
        created_at
        join_date
        email
        enabled
        id
        is_admin
        is_guest
        is_pending
        is_view_only
        is_verified
        location
        mobile_phone
        name
        phone
        photo_original
        photo_small
        photo_thumb
        photo_thumb_small
        photo_tiny
        time_zone_identifier
        title
        url
        utc_hours_diff
    }
}
"""

TEAMS = """
query {
    teams {
        id
        name
        picture_url
        users {
            id
        }
        owners {
            id
        }
    }
}
"""
