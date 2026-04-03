import asyncio
from app.database import AsyncSessionLocal
from app.models import Subscription


async def seed():
    async with AsyncSessionLocal() as session:
        # We check if it exists first so you can run this multiple times safely
        from sqlalchemy import select

        result = await session.execute(
            select(Subscription).where(Subscription.event_type == "user.signup")
        )
        existing = result.scalar_one_or_none()

        if not existing:
            new_sub = Subscription(
                event_type="user.signup",
                target_url="https://httpbin.org/post",
                secret="dev_secret_key_123",
            )
            session.add(new_sub)
            await session.commit()
            print("🌱 Seeded: 'user.signup' is now active.")
        else:
            print("✨ Subscription already exists. Skipping.")


if __name__ == "__main__":
    asyncio.run(seed())
