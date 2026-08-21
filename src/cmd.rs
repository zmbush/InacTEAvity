use std::collections::HashMap;
use std::fmt::Write as _;

use eyre::OptionExt as _;
use poise::serenity_prelude as serenity;

use crate::Context;

/// Displays the information about users that will be timed out soon.
#[poise::command(slash_command)]
pub(crate) async fn timing_out(ctx: Context<'_>) -> eyre::Result<()> {
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

/// Displays the information about users that will be timed out soon.
#[poise::command(slash_command, ephemeral)]
pub(crate) async fn config(
    ctx: Context<'_>,
    #[description = "The window of inactivity before timing out, in days. (default: 30)"]
    inactivity_window_days: Option<i64>,
    #[description = "Channel to notify about timeouts, a report will be generated once a day"]
    notification_channel: Option<serenity::Channel>,
    #[description = "The hour of the day to generate the report at (0-23) (UTC) (default: 12)"]
    generate_report_at_hour: Option<i64>,
    #[description = "The number of days before timeout a user is included in the report (default: 7)"]
    warning_threshold_days: Option<i64>,
) -> eyre::Result<()> {
    let Some(guild_id) = ctx.guild_id() else {
        ctx.say("This command can only be used in a guild".to_string())
            .await?;
        return Ok(());
    };
    let mut guild_data = ctx
        .data()
        .db
        .get_guild(guild_id)
        .await?
        .ok_or_eyre("Guild not in DB")?;
    let mut reindex = false;
    if let Some(inactivity_window_days) = inactivity_window_days {
        guild_data.inactivity_threshold_days = inactivity_window_days;
        reindex = true;
    }
    if let Some(channel) = notification_channel {
        guild_data.report_channel = Some(i64::from(channel.id()));
    }
    if let Some(generate_report_at_hour) = generate_report_at_hour {
        guild_data.generate_report_at_hour = generate_report_at_hour;
    }
    if let Some(warning_threshold_days) = warning_threshold_days {
        guild_data.warning_threshold_days = warning_threshold_days;
    }
    ctx.data().db.update_guild(guild_data).await?;
    if reindex {
        tokio::spawn({
            let http = ctx.serenity_context().http.clone();
            let data = ctx.data().clone();
            async move {
                if let Err(e) = data.index(&http, guild_id).await {
                    tracing::error!("Error indexing guild {guild_id}: {e:?}");
                }
            }
        });
    }
    ctx.say("Done.").await?;
    Ok(())
}
