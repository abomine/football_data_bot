from telegram import Update
from telegram.ext import ContextTypes
from typing import Optional
import logging

logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send welcome message with available commands"""
    help_text = """
⚽ Football Bot Help:
/fixtures [team] - Upcoming matches
/results [team] - Recent results (last 5)
/standings [league_id] - League table (default: PL)
    """
    await update.message.reply_text(help_text.strip())

async def fixtures(update: Update, context: ContextTypes.DEFAULT_TYPE, storage):
    """Handle fixtures command"""
    try:
        team_name = " ".join(context.args) if context.args else None
        fixtures = storage.get_upcoming_fixtures(team_name=team_name)

        if not fixtures:
            await update.message.reply_text("No upcoming fixtures found")
            return

        response = "🗓 Upcoming Fixtures:\n\n" + "\n\n".join(
            f"⚔ {f['home_team']} vs {f['away_team']}\n"
            f"📅 {f['date']}\n"
            f"🏟 {f.get('venue_name', 'Unknown venue')}"
            for f in fixtures[:10]  # Limit to 10 fixtures
        )
        await update.message.reply_text(response[:4096])  # Telegram max message length

    except Exception as e:
        logger.error(f"Fixtures error: {e}")
        await update.message.reply_text("❌ Error fetching fixtures")

async def results(update: Update, context: ContextTypes.DEFAULT_TYPE, storage):
    """Handle results command"""
    try:
        team_name = " ".join(context.args) if context.args else None
        results = storage.get_recent_results(team_name=team_name)

        if not results:
            await update.message.reply_text("No recent results found")
            return

        response = "📊 Recent Results:\n\n" + "\n\n".join(
            f"⚽ {r['home_team']} {r['home_goals']}-{r['away_goals']} {r['away_team']}\n"
            f"📅 {r['date']}"
            for r in results[:5]  # Last 5 results
        )
        await update.message.reply_text(response)

    except Exception as e:
        logger.error(f"Results error: {e}")
        await update.message.reply_text("❌ Error fetching results")


async def standings(update: Update, context: ContextTypes.DEFAULT_TYPE, storage):
    """Handle standings command"""
    try:
        league_id = (
            int(context.args[0]) if context.args else 39
        )  # Default: Premier League
        standings = storage.get_league_standings(league_id=league_id)

        if not standings:
            await update.message.reply_text("Standings not available for this league")
            return

        response = "🏆 League Standings:\n\n" + "\n".join(
            f"{pos}. {team['team_name']} - Pts: {team['points']} (GP: {team['games_played']})"
            for pos, team in enumerate(standings[:20], 1)  # Top 20 teams
        )
        await update.message.reply_text(response[:4096])

    except Exception as e:
        logger.error(f"Standings error: {e}")
        await update.message.reply_text("❌ Error fetching standings")
