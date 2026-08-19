use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

use ::serenity::{futures::StreamExt, model::channel::MessagesIter};
use eyre::{Context as _, OptionExt};
use poise::serenity_prelude::{self as serenity, Timestamp};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing_error::ErrorLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};

mod db;

const DATA_FILE: &str = "data.json";

#[derive(Debug)]
struct Data {
    guild_data: Arc<Mutex<BTreeMap<serenity::GuildId, GuildData>>>,
    db: db::Db,
}

impl Data {
    async fn persist(&self) -> Result<(), Error> {
        GuildData::persist(self.guild_data.lock().await)
    }

    async fn index<Ctx>(&self, ctx: Ctx, guild: serenity::GuildId) -> Result<(), Error>
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
                .add_user(member.user.id, member.user.bot)
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
                let mut messages = MessagesIter::<serenity::Http>::stream(ctx, id).boxed();

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
                        continue;
                    }
                    tracing::info!("Message: {} ({})", message.id, message.timestamp);
                    self.db
                        .add_user(message.author.id, message.author.bot)
                        .await
                        .context("while adding user")?;
                    self.db
                        .add_message(
                            message.id,
                            message.channel_id,
                            message.author.id,
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

#[derive(Default, Debug, Deserialize, Serialize)]
struct GuildData {
    last_event: BTreeMap<serenity::ChannelId, Timestamp>,
    users_last_seen: BTreeMap<serenity::UserId, Timestamp>,
}

impl GuildData {
    fn track(
        &mut self,
        user_id: serenity::UserId,
        channel_id: Option<serenity::ChannelId>,
        timestamp: Timestamp,
    ) {
        let previous = self
            .users_last_seen
            .entry(user_id)
            .or_insert_with(|| Timestamp::from_unix_timestamp(0).unwrap());
        *previous = std::cmp::max(*previous, timestamp);
        if let Some(channel_id) = channel_id {
            self.last_event
                .entry(channel_id)
                .and_modify(|e| *e = std::cmp::max(*e, timestamp))
                .or_insert(timestamp);
        }
    }

    fn persist(
        guard: tokio::sync::MutexGuard<'_, BTreeMap<serenity::GuildId, GuildData>>,
    ) -> Result<(), Error> {
        let file = std::fs::File::create(DATA_FILE)?;
        serde_json::to_writer_pretty(file, &*guard)?;
        Ok(())
    }

    async fn index<Ctx>(&mut self, ctx: Ctx, guild: serenity::GuildId) -> Result<(), Error>
    where
        Ctx: AsRef<serenity::Http> + serenity::CacheHttp + Copy,
    {
        let guild = guild.to_partial_guild(ctx).await?;
        tracing::info!("Indexing guild {} ({})", guild.name, guild.id);
        // Prefill with current members
        for member in guild.members(ctx, None, None).await? {
            self.track(
                member.user.id,
                None,
                member
                    .joined_at
                    .unwrap_or_else(|| Timestamp::from_unix_timestamp(0).unwrap()),
            );
        }
        for (id, channel) in guild.channels(ctx).await? {
            if channel.is_text_based() {
                let last_processed_in_channel = self.last_event.get(&id).cloned();

                tracing::info!(" - Indexing channel {} ({})", channel.name, id);
                let mut messages = MessagesIter::<serenity::Http>::stream(ctx, id).boxed();

                while let Some(message) = messages.next().await {
                    let Ok(message) = message else {
                        tracing::warn!("Error fetching message: {:?}", message);
                        break;
                    };
                    tracing::info!("Message: {} ({})", message.id, message.timestamp);
                    if *message.timestamp < chrono::Utc::now() - chrono::Duration::days(30) {
                        // Stop looking back, we've been here before, or it is beyond our vision.
                        break;
                    }
                    if let Some(last_event) = last_processed_in_channel
                        && message.timestamp <= last_event
                    {
                        // We've been here before.
                        break;
                    }
                    for reaction in &message.reactions {
                        for reactor in message
                            .reaction_users(ctx, reaction.reaction_type.clone(), None, None)
                            .await?
                        {
                            self.track(reactor.id, Some(id), message.timestamp);
                        }
                    }
                    self.track(message.author.id, Some(id), message.timestamp);
                }
            }
        }

        Ok(())
    }
}

type Error = eyre::Error;
//  Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

/// Displays your or another user's account creation date
#[poise::command(slash_command, prefix_command)]
async fn timing_out(ctx: Context<'_>) -> Result<(), Error> {
    ctx.defer_ephemeral().await?;

    tracing::info!("Running timing_out command");
    let data = ctx.data().guild_data.lock().await;
    tracing::info!("Locked data!");
    let Some(guild_id) = ctx.guild_id() else {
        ctx.say("This command can only be used in a guild".to_string())
            .await?;
        return Ok(());
    };
    let Some(guild_data) = data.get(&guild_id) else {
        ctx.say("No data for this guild yet".to_string()).await?;
        return Ok(());
    };
    let mut users_last_seen = guild_data.users_last_seen.iter().collect::<Vec<_>>();
    users_last_seen.sort_by_key(|(_, ts)| *ts);

    tracing::info!("Found {} users", users_last_seen.len());

    let mut timeout_msg = String::new();
    for (user_id, timestamp) in users_last_seen {
        if chrono::Utc::now() - **timestamp < chrono::Duration::days(30) {
            continue;
        }
        let Ok(user) = user_id.to_user(ctx).await else {
            tracing::warn!("Failed to fetch user {}", user_id);
            continue;
        };
        if user.bot {
            continue;
        }
        writeln!(
            timeout_msg,
            "<@{}> - {} days ago",
            user_id,
            (chrono::Utc::now() - **timestamp).num_days()
        )?;
    }
    if timeout_msg.len() > 1000 {
        timeout_msg.truncate(1000);
        timeout_msg.push_str("\n...and more");
    }
    tracing::info!("Replying with: \n{timeout_msg}");
    ctx.say(timeout_msg).await?;

    Ok(())
}

async fn event_handler(
    ctx: &serenity::Context,
    event: &serenity::FullEvent,
    data: &Data,
) -> Result<(), Error> {
    match event {
        serenity::FullEvent::Ready { data_about_bot } => {
            for guild in &data_about_bot.guilds {
                data.db.add_guild(guild.id).await?;
                data.index(ctx, guild.id).await.context("while indexing")?;
            }
        }
        serenity::FullEvent::Message { new_message } => {
            let Some(guild_id) = new_message.guild_id else {
                tracing::warn!("Message not in a guild: {:?}", new_message);
                return Ok(());
            };
            data.db
                .add_channel(new_message.channel_id, None, guild_id)
                .await?;
            data.db
                .add_user(new_message.author.id, new_message.author.bot)
                .await?;
            data.db
                .add_message(
                    new_message.id,
                    new_message.channel_id,
                    new_message.author.id,
                    new_message.timestamp,
                    new_message.edited_timestamp,
                )
                .await?;
        }
        serenity::FullEvent::ReactionAdd { add_reaction } => {
            let Some(guild_id) = add_reaction.guild_id else {
                tracing::warn!("Reaction not in a guild: {:?}", add_reaction);
                return Ok(());
            };
            let Some(user_id) = add_reaction.user_id else {
                tracing::warn!("Reaction has no user_id: {:?}", add_reaction);
                return Ok(());
            };
            let Some(message_author_id) = add_reaction.message_author_id else {
                tracing::warn!("Reaction has no message_author_id: {:?}", add_reaction);
                return Ok(());
            };
            data.db
                .add_channel(add_reaction.channel_id, None, guild_id)
                .await?;
            data.db.add_user(user_id, false).await?;
            data.db
                .add_message(
                    add_reaction.message_id,
                    add_reaction.channel_id,
                    message_author_id,
                    Timestamp::now(),
                    None,
                )
                .await?;
            data.db
                .add_reaction(add_reaction.message_id, user_id, Timestamp::now())
                .await?;
        }
        _ => {}
    }
    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::Registry::default()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(ErrorLayer::default())
        .init();

    dotenv::dotenv()?;
    let token = std::env::var("DISCORD_TOKEN").expect("missing DISCORD_TOKEN");
    let intents = serenity::GatewayIntents::GUILD_MEMBERS
        | serenity::GatewayIntents::GUILD_PRESENCES
        | serenity::GatewayIntents::GUILD_MESSAGES
        | serenity::GatewayIntents::GUILD_MESSAGE_REACTIONS;

    let db = db::Db::new().await?;

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![timing_out()],
            event_handler: |ctx, event, _framework, data| {
                Box::pin(async move { event_handler(ctx, event, data).await })
            },
            on_error: |error| {
                Box::pin(async move {
                    tracing::error!("Error in command: {:?}", error);
                })
            },
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                if let Ok(file) = std::fs::File::open(DATA_FILE) {
                    Ok(Data {
                        guild_data: Arc::new(Mutex::new(serde_json::from_reader(file)?)),
                        db,
                    })
                } else {
                    Ok(Data {
                        guild_data: Arc::new(Mutex::new(BTreeMap::new())),
                        db,
                    })
                }
            })
        })
        .build();

    let client = serenity::ClientBuilder::new(token, intents)
        .framework(framework)
        .await;
    client.unwrap().start().await.unwrap();

    Ok(())
}
