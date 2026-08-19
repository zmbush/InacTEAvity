use serenity::model::{
    Timestamp,
    id::{ChannelId, GuildId, MessageId, UserId},
};

const DB_FILE: &str = "sqlite:data.db";

#[derive(Debug)]
pub(crate) struct Db {
    pool: sqlx::SqlitePool,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct GuildData {
    pub(crate) id: i64,
    pub(crate) inactivity_threshold_days: i64,
    pub(crate) search_window_buffer_days: i64,
    pub(crate) report_channel: Option<i64>,
}

impl Db {
    pub(crate) async fn new() -> Result<Self, sqlx::Error> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(5)
            .connect(DB_FILE)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub(crate) async fn add_guild(&self, guild_id: GuildId) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO guilds (id, inactivity_threshold_days) VALUES (?, ?) ON CONFLICT(id) DO NOTHING",
            i64::from(guild_id),
            30
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn get_guild(
        &self,
        guild_id: GuildId,
    ) -> Result<Option<GuildData>, sqlx::Error> {
        sqlx::query_as!(
            GuildData,
            "SELECT * FROM guilds WHERE id = ?",
            i64::from(guild_id)
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub(crate) async fn add_user(&self, user_id: UserId, is_bot: bool) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO users (id, is_bot) VALUES (?, ?) ON CONFLICT(id) DO NOTHING",
            i64::from(user_id),
            is_bot
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn add_channel(
        &self,
        channel_id: ChannelId,
        channel_name: Option<&str>,
        guild_id: GuildId,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO channels (id, name, guild_id, name_old) VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET name = COALESCE(excluded.name, name)",
            i64::from(channel_id),
            channel_name,
            i64::from(guild_id),
            channel_name.unwrap_or("Unknown")
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn seen_message(&self, message_id: MessageId) -> Result<bool, sqlx::Error> {
        let result = sqlx::query!(
            "SELECT 1 as one FROM messages WHERE id = ?",
            i64::from(message_id)
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.is_some())
    }

    pub(crate) async fn add_message(
        &self,
        message_id: MessageId,
        channel_id: ChannelId,
        user_id: UserId,
        timestamp: Timestamp,
        edited_timestamp: Option<Timestamp>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO messages (id, channel_id, user_id, created_at, edited_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET edited_at = excluded.edited_at",
            i64::from(message_id),
            i64::from(channel_id),
            i64::from(user_id),
            timestamp.unix_timestamp(),
            edited_timestamp.map(|t| t.unix_timestamp())
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn add_reaction(
        &self,
        message_id: MessageId,
        user_id: UserId,
        timestamp: Timestamp,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO reactions (message_id, user_id, created_at) VALUES (?, ?, ?) ON CONFLICT(message_id, user_id) DO UPDATE SET created_at = excluded.created_at",
            i64::from(message_id),
            i64::from(user_id),
            timestamp.unix_timestamp()
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
