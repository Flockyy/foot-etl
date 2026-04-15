-- stg_matches.sql
-- Cleans the matches_19302010 seed:
--   • strips translations in parentheses from team names  (e.g. "Brazil (Brasil)" → "Brazil")
--   • strips trailing dot from venue                       (e.g. "Montevideo."    → "Montevideo")
--   • splits score into full-time and half-time columns    (e.g. "4-1 (3-0)")

with source as (
    select * from {{ ref('matches_19302010') }}
),

cleaned as (
    select
        edition,
        round,
        score,

        -- Remove " (NativeName)" suffix from team names
        trim(regexp_replace(team1, '\s*\(.*?\)', '')) as team1,
        trim(regexp_replace(team2, '\s*\(.*?\)', '')) as team2,

        -- Strip trailing dot from venue
        trim(trailing '.' from trim(venue))           as venue,

        year,
        url,

        -- Extract full-time score:  "4-1 (3-0)" → "4-1"
        regexp_extract(score, '(\d+-\d+)\s*\(', 1)   as full_time_score,

        -- Extract half-time score:  "4-1 (3-0)" → "3-0"
        regexp_extract(score, '\((\d+-\d+)\)', 1)     as half_time_score

    from source
    qualify row_number() over (partition by edition, team1, team2, score order by 1) = 1
)

select * from cleaned
