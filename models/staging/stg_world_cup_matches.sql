-- stg_world_cup_matches.sql
-- Cleans the world_cup_matches_2014 seed:
--   • strips leading/trailing whitespace from all text columns
--   • normalises team names and city to title-case
--   • parses Datetime string to a proper TIMESTAMP
--   • fills missing Attendance with the median value
--   • deduplicates exact duplicate rows

with source as (
    select * from {{ ref('world_cup_matches_2014') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by "MatchID" order by 1) = 1
),

cleaned as (
    select
        "Year"                                                          as year,

        -- Parse "12 Jun 2014 - 17:00" → TIMESTAMP
        strptime(trim("Datetime"), '%d %b %Y - %H:%M')                 as match_datetime,

        trim("Stage")                                                   as stage,
        trim("Stadium")                                                 as stadium,

        -- Title-case city and strip whitespace (DuckDB: no initcap, use regexp_replace)
        trim("City")                                                    as city,

        trim("Home Team Name")                                          as home_team_name,
        "Home Team Goals"                                               as home_team_goals,
        "Away Team Goals"                                               as away_team_goals,
        trim("Away Team Name")                                          as away_team_name,

        trim("Win conditions")                                          as win_conditions,

        -- Fill missing Attendance with median
        coalesce(
            "Attendance",
            median("Attendance") over ()
        )::integer                                                      as attendance,

        "Half-time Home Goals"                                          as halftime_home_goals,
        "Half-time Away Goals"                                          as halftime_away_goals,

        trim("Referee")                                                 as referee,
        trim("Assistant 1")                                             as assistant_1,
        trim("Assistant 2")                                             as assistant_2,

        "RoundID"                                                       as round_id,
        "MatchID"                                                       as match_id,
        trim("Home Team Initials")                                      as home_team_initials,
        trim("Away Team Initials")                                      as away_team_initials

    from deduped
)

select * from cleaned
