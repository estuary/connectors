from enum import StrEnum

class ProjectType(StrEnum):
    EMAIL_BASED = "Email-based"
    USER_ID_BASED = "UserID-based"
    HYBRID = "Hybrid"
