use std::collections::HashMap;

use serenity::model::{
    Timestamp,
    id::{ChannelId, GuildId, MessageId, UserId},
};

const DB_FILE: &str = "sqlite:data.db";

#[derive(Debug, Clone)]
pub(crate) struct Db {
    pool: sqlx::SqlitePool,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct GuildData {
    pub(crate) id: i64,
    pub(crate) inactivity_threshold_days: i64,
    pub(crate) search_window_buffer_days: i64,
    pub(crate) report_channel: Option<i64>,
    pub(crate) generate_report_at_hour: i64,
    pub(crate) warning_threshold_days: i64,
}

#[derive(Debug, sqlx::FromRow, Default)]
pub(crate) struct LastUserActivity {
    pub(crate) user_id: UserId,
    pub(crate) is_bot: bool,
    pub(crate) last_message_timestamp: Option<Timestamp>,
    pub(crate) last_reaction_timestamp: Option<Timestamp>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct Message {
    pub(crate) id: i64,
    pub(crate) channel_id: i64,
    pub(crate) user_id: Option<i64>,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) edited_at: Option<chrono::NaiveDateTime>,
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
        Self::add_guild_with(&self.pool, guild_id).await
    }

    pub(crate) async fn add_guild_with<'c, E>(e: E, guild_id: GuildId) -> Result<(), sqlx::Error>
    where
        E: sqlx::Executor<'c, Database = sqlx::Sqlite>,
    {
        sqlx::query!(
            "INSERT INTO guilds (id, inactivity_threshold_days) VALUES (?, ?) ON CONFLICT(id) DO NOTHING",
            i64::from(guild_id),
            30
        )
        .execute(e)
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

    pub(crate) async fn update_guild(&self, data: GuildData) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "UPDATE guilds SET inactivity_threshold_days = ?, search_window_buffer_days = ?, report_channel = ? WHERE id = ?",
            data.inactivity_threshold_days,
            data.search_window_buffer_days,
            data.report_channel,
            data.id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn add_user(
        &self,
        user_id: UserId,
        is_bot: bool,
        username: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO users (id, is_bot, username) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET username = COALESCE(excluded.username, username)",
            i64::from(user_id),
            is_bot,
            username,
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
            "INSERT INTO channels (id, name, guild_id) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET name = COALESCE(excluded.name, name)",
            i64::from(channel_id),
            channel_name,
            i64::from(guild_id),
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
        user_id: Option<UserId>,
        timestamp: Timestamp,
        edited_timestamp: Option<Timestamp>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "INSERT INTO messages (id, channel_id, user_id, created_at, edited_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET edited_at = excluded.edited_at",
            i64::from(message_id),
            i64::from(channel_id),
            user_id.map(|id| i64::from(id)),
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
            "INSERT INTO reactions
                (message_id, user_id, created_at)
            VALUES
                (?, ?, ?)
            ON CONFLICT(message_id, user_id) DO 
            UPDATE SET created_at = excluded.created_at",
            i64::from(message_id),
            i64::from(user_id),
            timestamp.unix_timestamp()
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub(crate) async fn oldest_message_in_channel(
        &self,
        channel_id: ChannelId,
    ) -> Result<Option<Message>, sqlx::Error> {
        sqlx::query_as!(
            Message,
            "SELECT * FROM messages WHERE channel_id = ? ORDER BY created_at ASC LIMIT 1",
            i64::from(channel_id)
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub(crate) async fn get_user_activity(
        &self,
        guild_id: GuildId,
    ) -> Result<Vec<LastUserActivity>, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;

        #[derive(sqlx::FromRow)]
        struct UserMessages {
            user_id: Option<i64>,
            last_message_timestamp: Option<chrono::NaiveDateTime>,
            is_bot: Option<bool>,
        }
        let user_messages = sqlx::query_as!(
            UserMessages,
            r#"
            SELECT
                m.user_id,
                MAX(COALESCE(m.edited_at, m.created_at)) as last_message_timestamp,
                u.is_bot as is_bot
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.id 
            WHERE m.channel_id IN (SELECT id FROM channels WHERE guild_id = ?)
            GROUP BY user_id;"#,
            i64::from(guild_id)
        )
        .fetch_all(&mut *transaction)
        .await?;

        #[derive(sqlx::FromRow)]
        struct UserReactions {
            user_id: Option<i64>,
            last_reaction_timestamp: Option<chrono::NaiveDateTime>,
            is_bot: Option<bool>,
        }
        let user_reactions = sqlx::query_as!(
            UserReactions,
            "SELECT
                r.user_id,
                MAX(created_at) as last_reaction_timestamp,
                u.is_bot as is_bot
            FROM reactions r
            LEFT JOIN users u ON r.user_id = u.id 
            WHERE r.message_id IN (SELECT id FROM messages WHERE channel_id IN (SELECT id FROM channels WHERE guild_id = ?)) GROUP BY user_id",
            i64::from(guild_id)
            )
            .fetch_all(&mut *transaction)
            .await?;

        let mut users = HashMap::<_, LastUserActivity>::new();
        for msg in user_messages {
            let Some(id) = msg.user_id else { continue };
            let Some(is_bot) = msg.is_bot else { continue };

            let entry = users.entry(id).or_default();
            entry.user_id = UserId::from(id as u64);
            entry.last_message_timestamp = msg
                .last_message_timestamp
                .map(|t| Timestamp::from_unix_timestamp(t.and_utc().timestamp()).unwrap());
            entry.is_bot = is_bot;
        }

        for reaction in user_reactions {
            let Some(id) = reaction.user_id else {
                continue;
            };
            let Some(is_bot) = reaction.is_bot else {
                continue;
            };

            let entry = users.entry(id).or_default();
            entry.user_id = UserId::from(id as u64);
            entry.last_reaction_timestamp = reaction
                .last_reaction_timestamp
                .map(|t| Timestamp::from_unix_timestamp(t.and_utc().timestamp()).unwrap());
            entry.is_bot = is_bot;
        }

        Ok(users.into_values().collect())
    }
}
