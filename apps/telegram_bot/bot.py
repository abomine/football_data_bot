import logging
from telegram.ext import Application, CommandHandler
import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Now import using absolute path from project root
from apps.telegram_bot import handlers
from pipelines.storage import FootballDataStorage

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class FootballBot:
    def __init__(self, token: str):
        self.storage = FootballDataStorage()
        self.app = Application.builder().token(token).build()
        self._register_handlers()

    def _register_handlers(self):
        """Register all command handlers"""
        self.app.add_handler(CommandHandler("start", handlers.start))
        self.app.add_handler(
            CommandHandler(
                "fixtures", lambda u, c: handlers.fixtures(u, c, self.storage)
            )
        )
        self.app.add_handler(
            CommandHandler("results", lambda u, c: handlers.results(u, c, self.storage))
        )
        self.app.add_handler(
            CommandHandler(
                "standings", lambda u, c: handlers.standings(u, c, self.storage)
            )
        )

    def run(self):
        """Run the bot indefinitely"""
        logger.info("Starting football bot...")
        self.app.run_polling()


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()

    bot = FootballBot(os.getenv("TELEGRAM_BOT_TOKEN"))
    bot.run()
