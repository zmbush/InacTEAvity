use eyre::{Context as _, OptionExt as _};
use futures::StreamExt as _;
use poise::serenity_prelude::{self as serenity};

use crate::db;

use super::Error;

#[derive(Debug, Clone)]
pub(crate) struct Data {
    pub(crate) db: db::Db,
}

impl Data {
    pub(crate) async fn index<Ctx>(&self, ctx: Ctx, guild: serenity::GuildId) -> Result<(), Error>
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

                tracing::info!(" - Indexing channel {} ({})", channel.name, id);
                let mut messages =
                    serenity::MessagesIter::<serenity::Http>::stream(ctx, id).boxed();

                while let Some(message) = messages.next().await {
                    let Ok(message) = message else {
                        tracing::warn!("Error fetching message: {:?}", message);
                        break;
                    };
                    if *message.timestamp < lookback {
                        // Stop looking back, we've been here before, or it is beyond our vision.
                        break;
                    }
                    if self.db.seen_message(message.id).await? {
                        // We've seen this message before.
                        break;
                    }
                    tracing::info!("Message: {} ({})", message.id, message.timestamp);
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
                }
            }
        }

        Ok(())
    }
}
