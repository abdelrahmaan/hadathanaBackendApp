"""Auth configuration: UserManager, transports, JWT strategy, and auth backend.

Uses fastapi-users v15 with:
- Cookie transport (HttpOnly, Secure, SameSite=lax)
- JWT strategy (HS256, configurable lifetime)
- Resend for transactional email (welcome + password-reset)
"""

import uuid

import resend
from fastapi import Depends, Request
from fastapi_users import BaseUserManager, FastAPIUsers, UUIDIDMixin
from fastapi_users.authentication import (
    AuthenticationBackend,
    CookieTransport,
    JWTStrategy,
)

from ..config import settings
from .database import MotorUserDatabase, get_user_db
from .models import User

SECRET = settings.jwt_secret


class UserManager(UUIDIDMixin, BaseUserManager[User, uuid.UUID]):
    reset_password_token_secret = SECRET
    verification_token_secret = SECRET

    async def on_after_register(
        self, user: User, request: Request | None = None
    ) -> None:
        if settings.resend_api_key:
            resend.api_key = settings.resend_api_key
            resend.Emails.send(
                {
                    "from": settings.from_email,
                    "to": [user.email],
                    "subject": "Welcome to Hadathana",
                    "html": "<p>Welcome! Your account has been created.</p>",
                }
            )

    async def on_after_forgot_password(
        self, user: User, token: str, request: Request | None = None
    ) -> None:
        reset_url = f"https://hadathana.app/reset-password?token={token}"
        if settings.resend_api_key:
            resend.api_key = settings.resend_api_key
            resend.Emails.send(
                {
                    "from": settings.from_email,
                    "to": [user.email],
                    "subject": "Reset your Hadathana password",
                    "html": (
                        f"<p>Click to reset your password "
                        f"(expires in {settings.reset_token_expire_minutes} minutes):</p>"
                        f"<p><a href='{reset_url}'>{reset_url}</a></p>"
                    ),
                }
            )


async def get_user_manager(
    user_db: MotorUserDatabase = Depends(get_user_db),
) -> UserManager:
    yield UserManager(user_db)


# ------------------------------------------------------------------
# Transport: HttpOnly cookie
# ------------------------------------------------------------------

cookie_transport = CookieTransport(
    cookie_name="access_token",
    cookie_max_age=settings.access_token_expire_minutes * 60,
    cookie_secure=True,
    cookie_httponly=True,
    cookie_samesite="lax",
)

# ------------------------------------------------------------------
# Strategy: JWT (HS256)
# ------------------------------------------------------------------


def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(
        secret=SECRET,
        lifetime_seconds=settings.access_token_expire_minutes * 60,
    )


# ------------------------------------------------------------------
# Backend + FastAPIUsers instance
# ------------------------------------------------------------------

auth_backend = AuthenticationBackend(
    name="cookie",
    transport=cookie_transport,
    get_strategy=get_jwt_strategy,
)

fastapi_users: FastAPIUsers[User, uuid.UUID] = FastAPIUsers(
    get_user_manager,
    [auth_backend],
)

# Dependency: inject the current active authenticated user (User model).
current_active_user = fastapi_users.current_user(active=True)
