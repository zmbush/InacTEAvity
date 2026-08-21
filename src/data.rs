use eyre::{Context as _, OptionExt as _};
use futures::StreamExt as _;
use poise::serenity_prelude::{self as serenity};

use crate::db;

#[derive(Debug, Clone)]
pub(crate) struct Data {
    pub(crate) db: db::Db,
}

impl Data {
    pub(crate) async fn index<Ctx>(&self, ctx: Ctx, guild: serenity::GuildId) -> eyre::Result<()>
    where
        Ctx: AsRef<serenity::Http> + serenity::CacheHttp + Copy,
    {
        let guild = guild.to_partial_guild(ctx).await?;
        let guild_config = self
            .db
            .get_guild(guild.id)
            .await?
            .ok_or_eyre("Guild not in DB")?;
        let lookback = chrono::Utc::now()
            - chrono::Duration::days(guild_config.inactivity_threshold_days)
            - chrono::Duration::days(guild_config.search_window_buffer_days);
        tracing::info!("Indexing guild {} ({})", guild.name, guild.id);
        // Prefill with current members
        for member in guild.members(ctx, None, None).await? {
            self.db
                .add_user(member.user.id, member.user.bot, Some(&member.user.name))
                .await
                .context("while adding user")?;
        }
        for (id, channel) in guild.channels(ctx).await? {
            if channel.is_text_based() {
                self.db
                    .add_channel(channel.id, Some(&channel.name), channel.guild_id)
                    .await
                    .context("while adding channel")?;

                tracing::info!(
                    " - [{}] Indexing channel {} ({})",
                    guild.name,
                    channel.name,
                    id
                );
                let oldest_in_channel = self.db.oldest_message_in_channel(id).await?;
                if let Some(oldest_in_channel) = oldest_in_channel {
                    if oldest_in_channel.created_at.and_utc() < lookback {
                        // We've already indexed this channel back to the lookback point, so we can skip it.
                        continue;
                    } else {
                        let mut oldest_id = serenity::MessageId::from(oldest_in_channel.id as u64);
                        // Index before this message, the loop below will get the most recent messages.
                        'searchback: loop {
                            let batch = id
                                .messages(
                                    ctx,
                                    serenity::builder::GetMessages::default().before(oldest_id),
                                )
                                .await?;

                            if batch.is_empty() {
                                break;
                            }
                            oldest_id = batch.last().unwrap().id;
                            for message in batch {
                                let keep_going = self
                                    .index_message(
                                        ctx,
                                        &guild.name,
                                        &channel.name,
                                        message,
                                        lookback,
                                    )
                                    .await?;
                                if !keep_going {
                                    break 'searchback;
                                }
                            }
                        }
                    }
                }

                let mut messages = id.messages_iter(ctx).boxed();
                while let Some(message) = messages.next().await {
                    let Ok(message) = message else {
                        tracing::warn!("Error fetching message: {:?}", message);
                        break;
                    };
                    let keep_going = self
                        .index_message(ctx, &guild.name, &channel.name, message, lookback)
                        .await?;
                    if !keep_going {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn index_message<Ctx>(
        &self,
        ctx: Ctx,
        guild_name: &str,
        channel_name: &str,
        message: serenity::Message,
        lookback: chrono::DateTime<chrono::Utc>,
    ) -> eyre::Result<bool>
    where
        Ctx: AsRef<serenity::Http> + serenity::CacheHttp + Copy,
    {
        if *message.timestamp < lookback {
            // Stop looking back, we've been here before, or it is beyond our vision.
            return Ok(false);
        }
        if self.db.seen_message(message.id).await? {
            // We've seen this message before.
            return Ok(false);
        }
        tracing::info!(
            "  % [{}][{}] Message: {} ({})",
            guild_name,
            channel_name,
            message.id,
            message.timestamp
        );
        self.db
            .add_user(
                message.author.id,
                message.author.bot,
                Some(&message.author.name),
            )
            .await
            .context("while adding user")?;
        self.db
            .add_message(
                message.id,
                message.channel_id,
                Some(message.author.id),
                message.timestamp,
                message.edited_timestamp,
            )
            .await
            .context("while adding message")?;
        for reaction in &message.reactions {
            for reactor in message
                .reaction_users(ctx, reaction.reaction_type.clone(), None, None)
                .await?
            {
                self.db
                    .add_reaction(message.id, reactor.id, message.timestamp)
                    .await
                    .context("while adding reaction")?;
            }
        }

        Ok(true)
    }
}
