use std::{collections::BTreeMap, fmt::Write as _, sync::Arc, time::Duration};

use poise::serenity_prelude::{self as serenity, Timestamp};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const DATA_FILE: &str = "data.json";

struct Data {
    guild_data: Arc<Mutex<BTreeMap<serenity::GuildId, GuildData>>>,
}

impl Data {
    async fn persist(&self) -> Result<(), Error> {
        let file = std::fs::File::create(DATA_FILE)?;
        serde_json::to_writer_pretty(file, &*self.guild_data.lock().await)?;
        Ok(())
    }
}

#[derive(Default, Debug, Deserialize, Serialize)]
struct GuildData {
    last_event: chrono::DateTime<chrono::Utc>,
    users_last_seen: BTreeMap<serenity::UserId, Timestamp>,
}

impl GuildData {
    fn track(&mut self, user_id: serenity::UserId, timestamp: Timestamp) {
        let previous = self
            .users_last_seen
            .entry(user_id)
            .or_insert_with(|| Timestamp::from_unix_timestamp(0).unwrap());
        *previous = std::cmp::max(*previous, timestamp);
        self.last_event = std::cmp::max(self.last_event, *timestamp);
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
                member
                    .joined_at
                    .unwrap_or_else(|| Timestamp::from_unix_timestamp(0).unwrap()),
            );
        }
        for (id, channel) in guild.channels(ctx).await? {
            if channel.is_text_based() {
                let mut oldest_seen_ts = chrono::Utc::now();
                let mut last_message_id = None;
                while oldest_seen_ts > chrono::Utc::now() - Duration::from_hours(24 * 30)
                    && oldest_seen_ts > self.last_event
                {
                    let messages = ctx
                        .as_ref()
                        .get_messages(id, last_message_id, Some(100))
                        .await?;
                    if messages.is_empty() {
                        break;
                    }
                    let last = messages.last().expect("messages is non-empty");
                    oldest_seen_ts = *last.timestamp;
                    last_message_id = Some(serenity::MessagePagination::Before(last.id));
                    for message in messages {
                        for reaction in &message.reactions {
                            for reactor in message
                                .reaction_users(ctx, reaction.reaction_type.clone(), None, None)
                                .await?
                            {
                                self.track(reactor.id, message.timestamp);
                            }
                        }
                        self.track(message.author.id, message.timestamp);
                    }
                }
            }
        }

        Ok(())
    }
}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

/// Displays your or another user's account creation date
#[poise::command(slash_command, prefix_command)]
async fn timing_out(ctx: Context<'_>) -> Result<(), Error> {
    let data = ctx.data().guild_data.lock().await;
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

    let mut timeout_msg = String::new();
    for (user_id, timestamp) in users_last_seen {
        if chrono::Utc::now() - **timestamp < chrono::Duration::days(30) {
            continue;
        }
        let user = user_id.to_user(ctx).await?;
        if user.bot {
            continue;
        }
        let last_seen_str = timestamp.format("%Y-%m-%d %H:%M:%S").to_string();
        writeln!(
            timeout_msg,
            "<@{}> was last seen at {} ({} days ago)",
            user_id,
            last_seen_str,
            (chrono::Utc::now() - **timestamp).num_days()
        )?;
    }
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
                let guild = guild.id.to_partial_guild(ctx).await?;
                data.guild_data
                    .lock()
                    .await
                    .entry(guild.id)
                    .or_default()
                    .index(ctx, guild.id)
                    .await?;
            }

            data.persist().await?;
            tracing::info!("Bot is ready! Data: {:?}", data.guild_data.lock().await);
        }
        serenity::FullEvent::Message { new_message } => {
            let Some(guild_id) = new_message.guild_id else {
                tracing::warn!("Message not in a guild: {:?}", new_message);
                return Ok(());
            };
            data.guild_data
                .lock()
                .await
                .entry(guild_id)
                .or_default()
                .track(new_message.author.id, new_message.timestamp);
            data.persist().await?;
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
            data.guild_data
                .lock()
                .await
                .entry(guild_id)
                .or_default()
                .track(user_id, Timestamp::now());
            data.persist().await?;
        }
        _ => {}
    }
    tracing::info!("{event:?}");
    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    dotenv::dotenv()?;
    let token = std::env::var("DISCORD_TOKEN").expect("missing DISCORD_TOKEN");
    let intents = serenity::GatewayIntents::GUILD_MEMBERS
        | serenity::GatewayIntents::GUILD_PRESENCES
        | serenity::GatewayIntents::GUILD_MESSAGES
        | serenity::GatewayIntents::GUILD_MESSAGE_REACTIONS;

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![timing_out()],
            event_handler: |ctx, event, _framework, data| {
                Box::pin(async move { event_handler(ctx, event, data).await })
            },
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                if let Ok(file) = std::fs::File::open(DATA_FILE) {
                    Ok(Data {
                        guild_data: Arc::new(Mutex::new(serde_json::from_reader(file)?)),
                    })
                } else {
                    Ok(Data {
                        guild_data: Arc::new(Mutex::new(BTreeMap::new())),
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
