use std::{collections::HashMap, fmt::Write as _};

use poise::serenity_prelude::{self as serenity, Timestamp};
use tracing_error::ErrorLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};

mod data;
mod db;

type Error = eyre::Error;
//  Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, data::Data, Error>;

/// Displays your or another user's account creation date
#[poise::command(slash_command, prefix_command)]
async fn timing_out(ctx: Context<'_>) -> Result<(), Error> {
    ctx.defer_ephemeral().await?;

    let Some(guild_id) = ctx.guild_id() else {
        ctx.say("This command can only be used in a guild".to_string())
            .await?;
        return Ok(());
    };
    let Some(guild_config) = ctx.data().db.get_guild(guild_id).await? else {
        ctx.say("This guild is not configured yet".to_string())
            .await?;
        return Ok(());
    };
    let mut all_users = guild_id
        .to_partial_guild(ctx)
        .await?
        .members(ctx, None, None)
        .await?
        .into_iter()
        .map(|member| (member.user.id, member.user.bot))
        .collect::<HashMap<_, _>>();
    let mut last_user_activity = ctx.data().db.get_user_activity(guild_id).await?;
    last_user_activity.sort_by_key(|a| a.last_message_timestamp.max(a.last_reaction_timestamp));
    let mut timeout_msg = String::new();
    for activity in last_user_activity {
        all_users.remove(&activity.user_id);

        if activity.is_bot {
            continue;
        }
        let last_activity = activity
            .last_message_timestamp
            .max(activity.last_reaction_timestamp);
        if let Some(activity_ts) = last_activity
            && *activity_ts
                > chrono::Utc::now()
                    - chrono::Duration::days(guild_config.inactivity_threshold_days)
        {
            tracing::info!(
                "Skipping user {} because they were active within the last {} days ({last_activity:?})",
                activity.user_id,
                guild_config.inactivity_threshold_days
            );
            continue;
        }
        writeln!(timeout_msg, "<@{}> - {:?}", activity.user_id, last_activity)?;
    }

    for (user_id, is_bot) in all_users {
        if is_bot {
            continue;
        }
        writeln!(
            timeout_msg,
            "<@{}> - before {} days",
            user_id, guild_config.inactivity_threshold_days
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
    data: &data::Data,
) -> Result<(), Error> {
    match event {
        serenity::FullEvent::Ready { data_about_bot } => {
            tracing::info!("Ready! Logged in as {}", data_about_bot.user.name);

            for guild in &data_about_bot.guilds {
                tokio::spawn({
                    let http = ctx.http.clone();
                    let data = data.clone();
                    let guild_id = guild.id;
                    async move {
                        if let Err(e) = data.db.add_guild(guild_id).await {
                            tracing::error!("Error adding guild {guild_id}: {e:?}");
                        }
                        if let Err(e) = data.index(&http, guild_id).await {
                            tracing::error!("Error indexing guild {guild_id}: {e:?}");
                        }
                    }
                });
            }
        }
        serenity::FullEvent::Message { new_message } => {
            let Some(guild_id) = new_message.guild_id else {
                tracing::warn!("Message not in a guild: {new_message:?}");
                return Ok(());
            };
            tracing::info!(
                "Message in guild {guild_id}: {} ({})",
                new_message.id,
                new_message.timestamp
            );
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
            tracing::info!(
                "Reaction in guild {guild_id}: {} ({})",
                add_reaction.message_id,
                add_reaction.channel_id
            );
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
                Ok(data::Data { db })
            })
        })
        .build();

    let client = serenity::ClientBuilder::new(token, intents)
        .framework(framework)
        .await;
    client.unwrap().start().await.unwrap();

    Ok(())
}
